import json
from pydantic_core import ValidationError
import asyncio

from db import get_collection
from aggregator import AggregationPipelineFactory
from models import AggregationQuery
from answers import WRONG_QUERY_ANSWER


with open("test_data.json", "r", encoding="utf-8") as f:
    test_data = json.loads(f.read())


async def message_handler_emulator(message: str):
    try:
        query = AggregationQuery.parse_raw(message)  # Validate input
    except ValidationError:
        return WRONG_QUERY_ANSWER

    factory = AggregationPipelineFactory(query)  # Generate pipeline
    pipeline = factory.make_aggregation_pipeline()

    collection = get_collection()  # Run aggregation
    result_values = []
    result_labels = []
    async for group in collection.aggregate(pipeline, allowDiskUse=True):
        result_values.append(group["total_value"])
        result_labels.append(str(group["label"]).replace(" ", "T"))

    return json.dumps({"dataset": result_values, "labels": result_labels})


async def test_wrong_input():
    input1 = "123"
    input2 = "{}"
    input3 = "{'k': 'value}"
    input4 = """{
        "dt_from": "2022-09-01T00:00:00",
        "dt_upto": "2022-12-31T23:59:00",
        "group_type": "year"
        }"""

    assert WRONG_QUERY_ANSWER == await message_handler_emulator(input1)
    assert WRONG_QUERY_ANSWER == await message_handler_emulator(input2)
    assert WRONG_QUERY_ANSWER == await message_handler_emulator(input3)
    assert WRONG_QUERY_ANSWER == await message_handler_emulator(input4)


async def test_aggregation_1():
    result = await message_handler_emulator(json.dumps(test_data["query1"]))
    data = json.loads(result)
    print(data)
    assert data == test_data["answer1"]


async def test_aggregation_2():
    result = await message_handler_emulator(json.dumps(test_data["query2"]))
    data = json.loads(result)
    print(data)
    assert data == test_data["answer2"]


async def test_aggregation_3():
    result = await message_handler_emulator(json.dumps(test_data["query3"]))
    data = json.loads(result)
    print(data)
    assert data == test_data["answer3"]


async def run_tests():
    await test_wrong_input()
    await test_aggregation_1()
    await test_aggregation_2()
    await test_aggregation_3()


if __name__ == "__main__":
    asyncio.run(run_tests())
