from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command
from pydantic_core import ValidationError

from models import AggregationQuery
from answers import WRONG_QUERY_ANSWER, START_ANSWER

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
    await message.answer("Input is valid")
