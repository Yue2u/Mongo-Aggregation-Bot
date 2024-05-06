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
        self.concats = {
            "month": self.__make_concat_by_month,
            "day": self.__make_concat_by_day,
            "hour": self.__make_concat_by_hour,
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

    def __make_concat_by_month(self):
        """Make conditions for concat operator by month"""
        return [
            {"$toString": "$_id.year"},
            "-",
            {"$toString": "$_id.month"},
            "-01T00:00:00",
        ]

    def __make_concat_by_day(self):
        """Make conditions for concat operator by day"""
        return [
            {"$toString": "$_id.year"},
            "-",
            {"$toString": "$_id.month"},
            "-",
            {"$toString": "$_id.day"},
            "T00:00:00",
        ]

    def __make_concat_by_hour(self):
        """Make conditions for concat operator by hour"""
        return [
            {"$toString": "$_id.year"},
            "-",
            {"$toString": "$_id.month"},
            "-",
            {"$toString": "$_id.day"},
            "T",
            {"$toString": "$_id.hour"},
            ":00:00",
        ]

    def __make_concat(self, grouping_type: str):
        """Make conditions for concat operator by grouping_type"""
        return self.concats[grouping_type]()

    def make_project_stage(self, grouping_type: str) -> dict:
        """Make conditions for project stage of aggregation"""
        return {
            "$project": {
                "_id": 0,
                "label": {
                    "$dateFromString": {
                        "dateString": {
                            "$concat": self.__make_concat(grouping_type),
                        },
                        "format": "%Y-%m-%dT%H:%M:%S",
                    }
                },
                "total_value": 1,
            }
        }

    def make_sort_stage(self):
        return {"$sort": {"label": 1}}

    def make_aggregation_pipeline(self):
        result = [
            self.make_densify(
                self.query.group_type, self.query.dt_from, self.query.dt_upto
            ),
            self.make_match_stage(self.query.dt_from, self.query.dt_upto),
            self.make_add_fields(),
            self.make_group_stage(self.query.group_type),
            self.make_project_stage(self.query.group_type),
            self.make_sort_stage(),
        ]
        return result
