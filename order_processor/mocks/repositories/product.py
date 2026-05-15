from mocks.models import ProductCategory
from typing import Any

class ProductRepository:
    def __init__(self):
        self._products: dict[str, dict[str, Any]] = {}

    async def get_product(self, product_id: str) -> dict[str, Any] | None:
        return self._products.get(product_id)

    async def get_products_by_category(self, category: ProductCategory) -> list[dict[str, Any]]:
        return [p for p in self._products.values() if p.get("category") == category.value]
