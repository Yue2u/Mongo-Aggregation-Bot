from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command
from pydantic_core import ValidationError
import json

from db import get_collection
from models import AggregationQuery
from answers import WRONG_QUERY_ANSWER, START_ANSWER
from aggregator import AggregationPipelineFactory

router = Router()


@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(START_ANSWER.format(message.from_user.username))


@router.message()
async def message_handler(message: Message):
    """"""
    try:
        query = AggregationQuery.parse_raw(message.text)  # Validate input
    except ValidationError:
        await message.answer(WRONG_QUERY_ANSWER)
        return

    factory = AggregationPipelineFactory(query)  # Generate pipeline
    pipeline = factory.make_aggregation_pipeline()

    collection = get_collection()  # Run aggregation
    result_values = []
    result_labels = []
    async for group in collection.aggregate(pipeline):
        result_values.append(group["total_value"])
        result_labels.append(str(group["label"]).replace(" ", "T"))

    await message.answer(
        json.dumps({"dataset": result_values, "labels": result_labels})
    )
