"""Модуль для создания базы данных."""

import asyncio

from models.database import create_db


async def create_database():
    """Функция создаёт базу данных."""
    await create_db()
    print('Create tables')


if __name__ == '__main__':
    asyncio.run(create_database())
