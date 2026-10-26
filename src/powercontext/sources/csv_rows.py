from __future__ import annotations

import csv
import io
import json
from urllib.parse import quote

from pydantic import BaseModel, field_validator, model_validator

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    TextEvidence,
)

CSV_ROW_SOURCE_NAME = "csv-row"


def _validate_document_id(value: str) -> str:
    if not value:
        raise ValueError("document_id must not be empty")
    if value != value.strip():
        raise ValueError("document_id must not have surrounding whitespace")
    return value


def _validate_primary_key(value: str) -> str:
    if not value or not value.strip():
        raise ValueError("primary_key must not be blank")
    return value


def _validate_primary_key_value(value: str) -> str:
    if not value or not value.strip():
        raise ValueError("primary_key_value must not be blank")
    return value


class CSVDocumentInput(BaseModel):
    document_id: str
    primary_key: str
    content: bytes

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        return _validate_document_id(value)

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, value: str) -> str:
        return _validate_primary_key(value)


class CSVRowInput(BaseModel):
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        return _validate_document_id(value)

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, value: str) -> str:
        return _validate_primary_key(value)

    @field_validator("primary_key_value")
    @classmethod
    def validate_primary_key_value(cls, value: str) -> str:
        return _validate_primary_key_value(value)

    @model_validator(mode="after")
    def validate_row(self) -> CSVRowInput:
        if not self.columns:
            raise ValueError("columns must not be empty")

        if any(not column or not column.strip() for column in self.columns):
            raise ValueError("column names must not be blank")

        if len(set(self.columns)) != len(self.columns):
            raise ValueError("column names must be unique")

        if self.primary_key not in self.columns:
            raise ValueError("primary_key must exactly match one column")

        if len(self.columns) != len(self.values):
            raise ValueError("columns and values must have equal length")

        index = self.columns.index(self.primary_key)
        if self.values[index] != self.primary_key_value:
            raise ValueError("primary_key_value does not match the row")

        return self


class CSVRowSource(Source):
    source_id: str
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @model_validator(mode="after")
    def validate_source(self) -> CSVRowSource:
        if self.name != self.source_id:
            raise ValueError("source name must equal source_id")

        if len(self.source_id) > MAX_SOURCE_ID_LENGTH:
            raise ValueError("source_id is too long")

        if self.materialization is not SourceMaterialization.CAPTURED:
            raise ValueError("CSV row sources must be CAPTURED")

        if not self.columns:
            raise ValueError("columns must not be empty")

        if len(self.columns) != len(self.values):
            raise ValueError("columns and values must have equal length")

        if self.primary_key not in self.columns:
            raise ValueError("primary_key must exactly match one column")

        index = self.columns.index(self.primary_key)
        if self.values[index] != self.primary_key_value:
            raise ValueError("primary_key_value does not match the row")

        return self
        
def csv_row_source_id(
    document_id: str,
    primary_key_value: str,
) -> str:
    _validate_document_id(document_id)
    _validate_primary_key_value(primary_key_value)

    source_id = (
        f"csv:{quote(document_id, safe='')}:"
        f"{quote(primary_key_value, safe='')}"
    )

    if len(source_id) > MAX_SOURCE_ID_LENGTH:
        raise ValueError("CSV row source_id is too long")

    return source_id


def parse_csv_document(
    value: CSVDocumentInput,
) -> tuple[CSVRowInput, ...]:
    _validate_document_id(value.document_id)
    _validate_primary_key(value.primary_key)

    try:
        text = value.content.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as error:
        raise ValueError("CSV content must be valid UTF-8") from error

    try:
        with io.StringIO(text, newline="") as buffer:
            records = list(
                csv.reader(
                    buffer,
                    delimiter=",",
                    quotechar='"',
                    doublequote=True,
                    strict=True,
                )
            )
    except csv.Error as error:
        raise ValueError("CSV content is malformed") from error

    if not records:
        raise ValueError("CSV document must contain a header")

    header = records[0]

    if not header:
        raise ValueError("CSV header must not be empty")

    if any(not column or not column.strip() for column in header):
        raise ValueError("CSV header contains a blank column")

    if len(set(header)) != len(header):
        raise ValueError("CSV header contains duplicate columns")

    if value.primary_key not in header:
        raise ValueError("primary_key must exactly match a CSV column")

    primary_key_index = header.index(value.primary_key)
    columns = tuple(header)
    seen_primary_keys: set[str] = set()
    rows: list[CSVRowInput] = []

    for record in records[1:]:
        if record == []:
            continue

        if len(record) != len(columns):
            raise ValueError("CSV record width does not match the header")

        primary_key_value = record[primary_key_index]
        _validate_primary_key_value(primary_key_value)

        if primary_key_value in seen_primary_keys:
            raise ValueError("CSV primary key values must be unique")

        seen_primary_keys.add(primary_key_value)

        csv_row_source_id(
            value.document_id,
            primary_key_value,
        )

        rows.append(
            CSVRowInput(
                document_id=value.document_id,
                primary_key=value.primary_key,
                primary_key_value=primary_key_value,
                columns=columns,
                values=tuple(record),
            )
        )

    return tuple(rows)
    
class CSVRowSourceAdapter:
    input_class = CSVRowInput
    name = CSV_ROW_SOURCE_NAME
    source_class = CSVRowSource

    async def resolve(
        self,
        value: CSVRowInput,
        /,
    ) -> CSVRowSource:
        source_id = csv_row_source_id(
            value.document_id,
            value.primary_key_value,
        )

        return CSVRowSource(
            name=source_id,
            source_id=source_id,
            materialization=SourceMaterialization.CAPTURED,
            document_id=value.document_id,
            primary_key=value.primary_key,
            primary_key_value=value.primary_key_value,
            columns=value.columns,
            values=value.values,
        )

    async def read(
        self,
        source: CSVRowSource,
        /,
    ) -> CSVRowInput:
        return CSVRowInput(
            document_id=source.document_id,
            primary_key=source.primary_key,
            primary_key_value=source.primary_key_value,
            columns=source.columns,
            values=source.values,
        )


class CSVRowTextEvidenceProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = CSVRowSource
    output_class = TextEvidence

    def project(
        self,
        source: CSVRowSource,
        /,
    ) -> TextEvidence:
        row = dict(
            zip(
                source.columns,
                source.values,
                strict=True,
            )
        )

        content = json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        return TextEvidence(
            source_type=CSV_ROW_SOURCE_NAME,
            source_id=source.source_id,
            content=content,
            metadata={
                "document_id": source.document_id,
                "primary_key": source.primary_key,
                "primary_key_value": source.primary_key_value,
                "columns": list(source.columns),
                "row": row,
            },
        )


CSV_ROW_SOURCE_DEFINITION: AdapterSourceDefinition[
    CSVRowInput,
    CSVRowSource,
    CSVRowInput,
] = AdapterSourceDefinition(
    CSVRowSourceAdapter(),
    version="1",
    projections=(CSVRowTextEvidenceProjection(),),
)


__all__ = [
    "CSV_ROW_SOURCE_DEFINITION",
    "CSV_ROW_SOURCE_NAME",
    "CSVDocumentInput",
    "CSVRowInput",
    "CSVRowSource",
    "CSVRowSourceAdapter",
    "CSVRowTextEvidenceProjection",
    "csv_row_source_id",
    "parse_csv_document",
]
