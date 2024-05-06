from models import AggregationQuery
from db import get_collection

from datetime import datetime, timedelta


class AggregationPipelineFactory:
    """Class that provides aggregation pipelines generation by query"""

    def __init__(self, query: AggregationQuery, *args, **kwargs):
        self.query = query
        self.collection = get_collection()
        self.__groupings = {
            "month": self.__make_group_by_month,
            "day": self.__make_group_by_day,
            "hour": self.__make_group_by_hour,
        }

    def make_match_stage(self, dt_from: datetime, dt_upto: datetime):
        """Make conditions for match stage of aggregation"""
        return {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}}

    def make_densify(self, grouping_type: str, dt_from: datetime, dt_upto: datetime):
        """Make densify stage for aggregation"""
        return {
            "$densify": {
                "field": "dt",
                "range": {
                    "step": 1,
                    "unit": grouping_type,
                    "bounds": [dt_from, dt_upto + timedelta(hours=1)],
                },
            }
        }

    def make_add_fields(self):
        """Add field 'value' = 0 to documents created by densify"""
        return {"$addFields": {"value": {"$ifNull": ["$value", 0]}}}

    def __make_group_by_month(self) -> dict[str, dict[str, str]]:
        """Make conditions for grouping by month"""
        return {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}}

    def __make_group_by_day(self) -> dict[str, dict[str, str]]:
        """Make conditions for grouping by day"""
        by_month: dict = self.__make_group_by_month()
        by_month["day"] = {"$dayOfMonth": "$dt"}
        return by_month

    def __make_group_by_hour(self) -> dict[str, dict[str, str]]:
        """Make conditions for grouping by hour"""
        by_day: dict = self.__make_group_by_day()
        by_day["hour"] = {"$hour": "$dt"}
        return by_day

    def __make_grouping(self, grouping_type: str) -> dict[str, dict[str, str]]:
        """Make conditions for group stage by grouping_type"""
        return self.__groupings[grouping_type]()

    def make_group_stage(self, grouping_type: str) -> dict:
        """Make conditions for group stage of aggregation"""
        return {
            "$group": {
                "_id": self.__make_grouping(grouping_type),
                "total_value": {"$sum": "$value"},
                "count": {"$sum": 1},
            }
        }
