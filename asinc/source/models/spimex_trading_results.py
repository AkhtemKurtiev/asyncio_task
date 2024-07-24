"""Модуль с описанием модели Spimex_trading_results."""

from datetime import datetime

from sqlalchemy import Column, Date, DateTime, Integer, String

from models.database import BaseModel


class Spimex_trading_results(BaseModel):
    """Класс модели Spimex_trading_results."""

    __tablename__ = 'spimex_trading_results'

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String(15))
    exchange_product_name = Column(String(256))
    oil_id = Column(String(4))
    delivery_basis_id = Column(String(4))
    delivery_basis_name = Column(String(256))
    delivery_type_id = Column(String(1))
    volume = Column(Integer)
    total = Column(Integer)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime, default=datetime.now())
    updated_on = Column(
        DateTime,
        default=datetime.now(),
        onupdate=datetime.now()
    )
