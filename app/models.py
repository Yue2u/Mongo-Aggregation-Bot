from enum import Enum
from pydantic import BaseModel
from datetime import datetime


class AggregationPeriod(str, Enum):
    month = "month"
    day = "day"
    hour = "hour"


class AggregationQuery(BaseModel):
    dt_from: datetime
    dt_upto: datetime
    group_type: AggregationPeriod

    class Config:
        use_enum_values = True
