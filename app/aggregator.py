from models import AggregationQuery
from db import get_collection

from datetime import datetime, timedelta


class AggregationPipelineFactory:
    """Class that provides aggregation pipelines generation by query"""

    def __init__(self, query: AggregationQuery, *args, **kwargs):
        self.query = query
        self.collection = get_collection()

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
