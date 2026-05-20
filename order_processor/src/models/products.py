from sqlalchemy import Column, String, JSON

from .base import Base


class Product(Base):
    __tablename__ = "products"

    id = Column(String, primary_key=True)
    category = Column(String, nullable=True)
    data = Column(JSON, nullable=True)
