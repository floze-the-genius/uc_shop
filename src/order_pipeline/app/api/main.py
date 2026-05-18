from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query

from order_pipeline.app.api.dependencies import create_order_use_case, create_store
from order_pipeline.app.api.responses import order_response
from order_pipeline.app.api.schemas import CreateOrderRequest


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = await create_store()
    app.state.create_order = create_order_use_case(app.state.store)
    try:
        yield
    finally:
        await app.state.store.close()


app = FastAPI(title="UC Shop Kafka Order API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/orders", status_code=202)
async def create_order(
    request: CreateOrderRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    trace_id: str | None = Header(default=None, alias="X-Trace-Id"),
) -> dict[str, Any]:
    order = await app.state.create_order.execute(
        category=request.category,
        telegram_id=request.telegram_id,
        payload=request.payload,
        idempotency_key=idempotency_key,
        trace_id=trace_id,
    )
    return {"accepted": True, "order": order_response(order)}


@app.get("/orders/{order_id}")
async def get_order(order_id: int) -> dict[str, Any]:
    order = await app.state.store.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order_response(order)


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=50, ge=1, le=200)
) -> list[dict[str, Any]]:
    return [order_response(order) for order in await app.state.store.list_orders(limit)]
