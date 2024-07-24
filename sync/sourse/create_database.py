"""Модуль для создания базы данных."""

from models.database import create_db
from models.spimex_trading_results import Spimex_trading_results


def create_database():
    """Функция создаёт базу данных."""
    create_db()
    print('Create tables')


if __name__ == '__main__':
    create_database()
