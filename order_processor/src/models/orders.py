from sqlalchemy import Column, String, Boolean, JSON, DateTime
from sqlalchemy.sql import func

from .base import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(String, primary_key=True)
    status = Column(String, nullable=False)
    _metadata = Column("metadata", JSON, nullable=True)
    product_category = Column(String, nullable=True)
    is_w_telegram_id = Column(Boolean, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
