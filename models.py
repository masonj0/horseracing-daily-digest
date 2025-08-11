from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator


class RunnerForm(BaseModel):
    pos: Optional[int] = None
    btn: Optional[float] = None
    going: Optional[str] = None
    dist_m: Optional[int] = None
    cls: Optional[str] = None
    date: Optional[str] = None


class RunnerFeatures(BaseModel):
    name: str
    jockey: Optional[str] = None
    trainer: Optional[str] = None
    form: List[RunnerForm] = Field(default_factory=list)
    j_win: Optional[float] = None
    t_win: Optional[float] = None
    jt_win: Optional[float] = None
    rating: Optional[float] = None
    at_course: Optional[float] = None
    at_distance: Optional[float] = None
    going_profile: Optional[float] = None


class RunnerRecord(BaseModel):
    race_id: str
    name: str
    upi_score: Optional[float] = None
    features: RunnerFeatures = Field(default_factory=RunnerFeatures)
    odds_str: Optional[str] = None


class RaceRecord(BaseModel):
    id: str
    course: str
    country: str
    discipline: str
    utc_datetime: str
    local_time: str
    timezone_name: str
    field_size: int
    value_score: Optional[float] = None
    runners: List[RunnerRecord] = Field(default_factory=list)


class OddsSnapshot(BaseModel):
    race_id: str
    runner_name: str
    odds: Optional[float] = None
    ts: str


class MarketEvent(BaseModel):
    race_id: str
    runner_name: str
    kind: str  # steamer|drifter
    from_odds: Optional[float] = None
    to_odds: Optional[float] = None
    ts: str