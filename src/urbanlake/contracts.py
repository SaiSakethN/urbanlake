"""Versioned observations: a source snapshot is not a source change event."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class SourceRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    unique_key: str = Field(pattern=r"^\d+$")
    created_date: datetime

    @field_validator("created_date")
    @classmethod
    def local_source_time(cls, value: datetime) -> datetime:
        if value.tzinfo is not None:
            raise ValueError("Socrata created_date must be local floating NYC time")
        return value


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    observation_id: str
    run_id: str
    kind: Literal["request", "daily_demand"]
    source_key: str
    observed_at: datetime
    fingerprint: str
    raw_json: str

    @field_validator("observed_at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("observation time must be timezone aware")
        return value.astimezone(UTC)

    @classmethod
    def wrap(cls, row: dict, *, kind: str, run_id: str, observed_at: datetime):
        if kind == "request":
            SourceRequest.model_validate(row)
            key = row["unique_key"]
        else:
            datetime.strptime(row["day"][:10], "%Y-%m-%d")
            if int(row["requests"]) < 0:
                raise ValueError("negative daily count")
            key = row["day"][:10]
        raw = canonical(row)
        fingerprint = digest(raw)
        return cls(
            observation_id=digest(f"{run_id}:{kind}:{key}:{fingerprint}"),
            run_id=run_id,
            kind=kind,
            source_key=key,
            observed_at=observed_at,
            fingerprint=fingerprint,
            raw_json=raw,
        )
