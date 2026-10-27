# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Capture Pi v3 session branches as immutable, event-level evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn

from pydantic import BaseModel, ConfigDict, JsonValue, field_validator

from powercontext.sources.connectors import (
    ConnectorRunCompletion,
    ConnectorRunSession,
    ConnectorRunStatus,
)
from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

PI_EVENT_SOURCE_NAME = "pi-event"
PI_SESSION_CONNECTOR_NAME = "pi-session-history"
PI_SESSION_CONNECTOR_VERSION = "1"
_CHECKPOINT_VERSION = 1
_DIGEST_PREFIX = "sha256:"


class PiSessionInput(BaseModel):
    """Raw Pi v3 JSONL and the optional selected branch leaf."""

    model_config = ConfigDict(frozen=True)

    content: bytes
    leaf_id: str | None = None

    @field_validator("leaf_id")
    @classmethod
    def validate_leaf_id(cls, value: str | None) -> str | None:
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError("leaf_id must be a non-empty string when supplied")  # noqa: TRY003
        return value


class PiEventInput(BaseModel):
    """One visible Pi event with the complete source record retained."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, JsonValue]

    @field_validator("session_id", "event_id", "event_type", "role")
    @classmethod
    def validate_non_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("event identity and text fields must not be blank")  # noqa: TRY003
        return value


class PiEventSource(Source):
    """Durable captured representation of one Pi event."""

    materialization: Literal[SourceMaterialization.CAPTURED] = SourceMaterialization.CAPTURED
    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, JsonValue]


class PiEventSourceAdapter:
    """Resolve and read Pi events without exposing persistence details."""

    input_class = PiEventInput
    name = PI_EVENT_SOURCE_NAME
    source_class = PiEventSource

    async def resolve(self, value: PiEventInput, /) -> PiEventSource:
        return PiEventSource(
            name=event_source_name(value.session_id, value.event_id),
            materialization=SourceMaterialization.CAPTURED,
            session_id=value.session_id,
            event_id=value.event_id,
            parent_id=value.parent_id,
            event_type=value.event_type,
            role=value.role,
            content=value.content,
            record=value.record,
        )

    async def read(self, source: PiEventSource, /) -> PiEventInput:
        return PiEventInput(
            session_id=source.session_id,
            event_id=source.event_id,
            parent_id=source.parent_id,
            event_type=source.event_type,
            role=source.role,
            content=source.content,
            record=source.record,
        )


class PiEventTextEvidenceProjection:
    """Expose one Pi event through the standard text evidence contract."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PiEventSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PiEventSource, /) -> TextEvidence:
        return TextEvidence(
            source_type=PI_EVENT_SOURCE_NAME,
            source_id=source.name,
            content=source.content,
            metadata={
                "session_id": source.session_id,
                "event_id": source.event_id,
                "parent_id": source.parent_id,
                "event_type": source.event_type,
                "role": source.role,
            },
        )


PI_EVENT_SOURCE_ADAPTER = PiEventSourceAdapter()
PI_EVENT_SOURCE_DEFINITION = AdapterSourceDefinition(
    PI_EVENT_SOURCE_ADAPTER,
    projections=(PiEventTextEvidenceProjection(),),
)


@dataclass(frozen=True, slots=True)
class _ParsedSession:
    session_id: str
    records: tuple[dict[str, JsonValue], ...]
    selected_records: tuple[dict[str, JsonValue], ...]


@dataclass(frozen=True, slots=True)
class _CheckpointEntry:
    session_id: str
    event_id: str
    digest: str


def parse_pi_session(value: PiSessionInput, /) -> tuple[PiEventInput, ...]:
    """Return visible events on the selected Pi branch from root to leaf."""

    parsed = _parse_session(value.content, value.leaf_id)
    return tuple(
        event for record in parsed.selected_records if (event := _visible_event(record, parsed.session_id)) is not None
    )


class PiSessionConnector:
    """Incrementally capture the visible events on one selected Pi branch."""

    name = PI_SESSION_CONNECTOR_NAME
    version = PI_SESSION_CONNECTOR_VERSION
    source_definitions = frozenset({PI_EVENT_SOURCE_NAME})

    def __init__(self, path: str | Path, leaf_id: str | None = None) -> None:
        self.path = Path(path)
        self.leaf_id = leaf_id

    async def run(self, session: ConnectorRunSession, /) -> ConnectorRunCompletion:
        parsed = _parse_session(self.path.read_bytes(), self.leaf_id)
        imported = _decode_checkpoint(session.checkpoint)
        visible_records: dict[tuple[str, str], tuple[PiEventInput, dict[str, JsonValue]]] = {}
        for record in parsed.records:
            event = _visible_event(record, parsed.session_id)
            if event is not None:
                visible_records[(event.session_id, event.event_id)] = (event, record)

        for entry in imported.values():
            visible = visible_records.get((entry.session_id, entry.event_id))
            if visible is None or _record_digest(visible[1]) != entry.digest:
                raise ValueError(  # noqa: TRY003
                    f"already imported Pi event {entry.session_id!r}/{entry.event_id!r} was changed"
                )

        for record in parsed.selected_records:
            event_id = _required_id(record.get("id"), "event")
            visible = visible_records.get((parsed.session_id, event_id))
            if visible is None:
                continue
            event, raw_record = visible
            key = (event.session_id, event.event_id)
            if key in imported:
                continue
            outcome = await session.submit(
                event_source_name(event.session_id, event.event_id),
                PI_EVENT_SOURCE_NAME,
                event,
            )
            if outcome.source_ref is not None:
                imported[key] = _CheckpointEntry(
                    session_id=event.session_id,
                    event_id=event.event_id,
                    digest=_record_digest(raw_record),
                )

        return ConnectorRunCompletion(
            status=ConnectorRunStatus.COMPLETE,
            checkpoint=_encode_checkpoint(imported),
        )


def event_source_name(session_id: str, event_id: str) -> str:
    """Build a bounded identity determined only by the session and event IDs."""

    identity = json.dumps(
        [session_id, event_id],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{PI_EVENT_SOURCE_NAME}:{hashlib.sha256(identity).hexdigest()}"


def _parse_session(content: bytes, leaf_id: str | None) -> _ParsedSession:
    records = _decode_jsonl(content)
    session_id = _session_id(records)
    event_records, by_id, ordered_ids = _index_events(records[1:])
    return _ParsedSession(
        session_id=session_id,
        records=event_records,
        selected_records=_select_branch(by_id, ordered_ids, leaf_id),
    )


def _session_id(records: list[dict[str, JsonValue]]) -> str:
    if not records:
        raise ValueError("Pi session must start with a session record")  # noqa: TRY003
    header = records[0]
    if header.get("type") != "session":
        raise ValueError("the first Pi record must have type=session")  # noqa: TRY003
    version = header.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version != 3:
        raise ValueError("Pi session version must be 3")  # noqa: TRY003
    return _required_id(header.get("id"), "session")


def _index_events(
    event_records: list[dict[str, JsonValue]],
) -> tuple[
    tuple[dict[str, JsonValue], ...],
    dict[str, dict[str, JsonValue]],
    list[str],
]:
    by_id: dict[str, dict[str, JsonValue]] = {}
    ordered_ids: list[str] = []
    for record in event_records:
        event_id = _required_id(record.get("id"), "event")
        if event_id in by_id:
            raise ValueError(f"duplicate Pi event id {event_id!r}")  # noqa: TRY003
        if "parentId" not in record:
            raise ValueError(f"Pi event {event_id!r} is missing parentId")  # noqa: TRY003
        parent_id = _normalize_parent(record.get("parentId"), event_id)
        if parent_id is not None and parent_id not in by_id:
            raise ValueError(  # noqa: TRY003
                f"Pi event {event_id!r} references parent {parent_id!r} before it appears"
            )
        by_id[event_id] = record
        ordered_ids.append(event_id)
    return tuple(event_records), by_id, ordered_ids


def _select_branch(
    by_id: dict[str, dict[str, JsonValue]],
    ordered_ids: list[str],
    leaf_id: str | None,
) -> tuple[dict[str, JsonValue], ...]:
    if leaf_id is None:
        selected_leaf = ordered_ids[-1] if ordered_ids else None
    else:
        selected_leaf = leaf_id
        if selected_leaf not in by_id:
            raise ValueError(f"selected Pi leaf {selected_leaf!r} does not exist")  # noqa: TRY003

    ancestors: list[dict[str, JsonValue]] = []
    seen: set[str] = set()
    current = selected_leaf
    while current is not None:
        if current in seen:
            raise ValueError(f"cycle detected in Pi history at {current!r}")  # noqa: TRY003
        seen.add(current)
        record = by_id[current]
        ancestors.append(record)
        current = _normalize_parent(record.get("parentId"), current)
    ancestors.reverse()
    return tuple(ancestors)


def _decode_jsonl(content: bytes) -> list[dict[str, JsonValue]]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("Pi session must be valid UTF-8") from error  # noqa: TRY003

    records: list[dict[str, JsonValue]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line, parse_constant=_reject_json_constant)
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid Pi JSON on line {line_number}") from error  # noqa: TRY003
        if not isinstance(record, dict):
            raise ValueError(  # noqa: TRY003, TRY004
                f"Pi record on line {line_number} must be a JSON object"
            )
        records.append(record)
    return records


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"invalid JSON constant {value}")  # noqa: TRY003


def _required_id(value: object, kind: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Pi {kind} id must be a non-empty string")  # noqa: TRY003
    return value


def _normalize_parent(value: object, event_id: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Pi event {event_id!r} has an invalid parentId")  # noqa: TRY003
    return value


def _visible_event(record: dict[str, JsonValue], session_id: str) -> PiEventInput | None:
    event_type = record["type"]
    if not isinstance(event_type, str) or not event_type:
        raise ValueError("Pi event type must be a non-empty string")  # noqa: TRY003

    if event_type == "message":
        visible = _message_body(record.get("message"))
    elif event_type in {"compaction", "branch_summary"}:
        summary = record.get("summary")
        visible = (event_type, summary) if isinstance(summary, str) and summary else None
    elif event_type == "custom_message":
        text = _visible_text(record.get("content"))
        visible = ("custom", text) if text else None
    else:
        visible = None

    if visible is None:
        return None
    role, body = visible
    event_id = _required_id(record.get("id"), "event")
    parent_id = _normalize_parent(record.get("parentId"), event_id)
    return PiEventInput(
        session_id=session_id,
        event_id=event_id,
        parent_id=parent_id,
        event_type=event_type,
        role=role,
        content=f"{role}: {body}",
        record=record,
    )


def _message_body(value: JsonValue) -> tuple[str, str] | None:
    if not isinstance(value, dict):
        return None
    role = value.get("role")
    if not isinstance(role, str) or not role:
        return None

    if role == "bashExecution":
        command = value.get("command")
        output = value.get("output")
        if not isinstance(command, str) or not command:
            return None
        body = f"$ {command}"
        if "output" in value:
            body += f"\n{_text_value(output)}"
        return role, body

    if role == "toolResult":
        text = _visible_text(value.get("content"))
        if not text:
            return None
        tool_name = value.get("toolName")
        tool_call_id = value.get("toolCallId")
        name = tool_name if isinstance(tool_name, str) else ""
        call_id = tool_call_id if isinstance(tool_call_id, str) else ""
        return role, f"[tool_result {name} {call_id}] {text}"

    body = _message_content(value.get("content"))
    if body is None or body == "":
        return None
    return role, body


def _message_content(value: JsonValue | None) -> str | None:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return None

    parts: list[str] = []
    for block in value:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text":
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
        elif block_type == "toolCall":
            name = block.get("name")
            call_id = block.get("id")
            name_text = name if isinstance(name, str) else ""
            call_id_text = call_id if isinstance(call_id, str) else ""
            arguments = block.get("arguments", {})
            parts.append(f"[tool_call {name_text} {call_id_text}] {_compact_json(arguments)}")
    if not parts:
        return None
    return "\n".join(parts)


def _visible_text(value: JsonValue | None) -> str | None:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return None
    parts = [
        text
        for block in value
        if isinstance(block, dict) and block.get("type") == "text" and isinstance((text := block.get("text")), str)
    ]
    return "\n".join(parts) if parts else None


def _text_value(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        text = _visible_text(value)
        return text if text is not None else ""
    return ""


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _record_digest(record: dict[str, JsonValue]) -> str:
    encoded = _compact_json(record).encode("utf-8")
    return f"{_DIGEST_PREFIX}{hashlib.sha256(encoded).hexdigest()}"


def _decode_checkpoint(checkpoint: JsonValue | None) -> dict[tuple[str, str], _CheckpointEntry]:
    if checkpoint is None:
        return {}
    if not isinstance(checkpoint, dict):
        raise ValueError("Pi connector checkpoint must be an object")  # noqa: TRY003, TRY004
    if checkpoint.get("version") != _CHECKPOINT_VERSION:
        raise ValueError("unsupported Pi connector checkpoint version")  # noqa: TRY003
    events = checkpoint.get("events")
    if not isinstance(events, list):
        raise ValueError("Pi connector checkpoint events must be a list")  # noqa: TRY003, TRY004

    decoded: dict[tuple[str, str], _CheckpointEntry] = {}
    for value in events:
        if not isinstance(value, dict):
            raise ValueError("Pi connector checkpoint entries must be objects")  # noqa: TRY003, TRY004
        session_id = _required_id(value.get("session_id"), "checkpoint session")
        event_id = _required_id(value.get("event_id"), "checkpoint event")
        digest = value.get("digest")
        if not isinstance(digest, str) or not _valid_digest(digest):
            raise ValueError("Pi connector checkpoint entry has an invalid digest")  # noqa: TRY003
        key = (session_id, event_id)
        if key in decoded:
            raise ValueError("Pi connector checkpoint contains a duplicate event")  # noqa: TRY003
        decoded[key] = _CheckpointEntry(session_id=session_id, event_id=event_id, digest=digest)
    return decoded


def _encode_checkpoint(imported: dict[tuple[str, str], _CheckpointEntry]) -> dict[str, JsonValue]:
    events: list[JsonValue] = [
        {
            "session_id": entry.session_id,
            "event_id": entry.event_id,
            "digest": entry.digest,
        }
        for entry in sorted(imported.values(), key=lambda item: (item.session_id, item.event_id))
    ]
    return {"version": _CHECKPOINT_VERSION, "events": events}


def _valid_digest(value: str) -> bool:
    if not value.startswith(_DIGEST_PREFIX):
        return False
    digest = value.removeprefix(_DIGEST_PREFIX)
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


__all__ = [
    "PI_EVENT_SOURCE_DEFINITION",
    "PI_EVENT_SOURCE_NAME",
    "PI_SESSION_CONNECTOR_NAME",
    "PI_SESSION_CONNECTOR_VERSION",
    "PiEventInput",
    "PiEventSource",
    "PiSessionConnector",
    "PiSessionInput",
    "parse_pi_session",
]
