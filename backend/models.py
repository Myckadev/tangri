from pydantic import BaseModel, Field, validator
from typing import List
import re
from datetime import datetime, timezone

_slug_re = re.compile(r"[^a-z0-9\-]+")

def slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\s+", "-", s)
    s = _slug_re.sub("", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class City(BaseModel):
    id: str = Field(default="", pattern=r"^[a-z0-9][a-z0-9\-]{1,62}[a-z0-9]$")
    name: str = Field(min_length=1)
    country: str = Field(min_length=2, max_length=2)  # ISO2
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    active: bool = True

    @validator("id", always=True)
    def ensure_id(cls, v, values):
        if v:
            return v
        name = values.get("name", "")
        country = values.get("country", "")
        base = f"{name}-{country}" if country else name
        s = slugify(base)
        if not s:
            raise ValueError("invalid id (empty after slugify)")
        return s

class ConfigDoc(BaseModel):
    version: int = 1
    updated_at: str = Field(default_factory=now_iso)
    updated_by: str = "backend"
    cities: List[City] = []
