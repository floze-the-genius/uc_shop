"""
Дано:
    - обработка заказов на apscheduler
    - apscheduler запускается в одном экземпляре
    - псевдокод ниже

Задача:
    - проанализировать код ниже (плюсы, минусы)
    - предложить альтернативу
    - написать альтернативное решение в репе (запуск через docker-compose)

"""

import asyncio
import json
import logging
import time
import traceback
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.controllers.orders_controller import OrdersController
from app.models.orders import OrderStatus, OrderUpdate, OrderDict
from app.models.product import ProductCategory
from app.modules.config import config
from app.modules.database.repositories.orders_repo import OrdersRepository
from app.modules.database.repositories.product_repo import ProductRepository
from app.modules.order_processors.factory import OrderProcessorFactory
from app.modules.order_processors.order_processor_utils import OrderProcessorUtils

logger = logging.getLogger(__name__)


class OrderProcessorScheduler:
    def __init__(self, interval_seconds_not_tg=10, interval_seconds_tg=30, interval_seconds_restart=60):
        self.scheduler = AsyncIOScheduler()
        self.interval_seconds_not_tg = interval_seconds_not_tg
        self.interval_seconds_tg = interval_seconds_tg
        self.interval_seconds_restart = interval_seconds_restart
        self.processing_lock_ttl_seconds = 600
        self.max_parallel_orders = 10
        self.orders_repo = None
        self.orders_controller = None
        self.product_repo = None
        self.bulk_limit = 25

    def _process_metadata(self, metadata):
        if metadata is None:
            return {}

        if isinstance(metadata, dict):
            return OrderProcessorUtils.sanitize_for_json(metadata.copy())

        if isinstance(metadata, str):
            try:
                parsed = json.loads(metadata)
                return OrderProcessorUtils.sanitize_for_json(parsed)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Could not parse metadata string: {metadata}")
                return {}

        logger.warning(f"Unexpected metadata type: {type(metadata)}")
        return {}

    @staticmethod
    def _parse_iso_datetime(value):
        if not value or not isinstance(value, str):
            return None

        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except (TypeError, ValueError):
            return None

    def _is_processing_lock_stale(self, metadata):
        if not metadata.get("processing_lock"):
            return False

        last_attempt = self._parse_iso_datetime(metadata.get("last_processing_attempt"))
        if last_attempt is None:
            return True

        now = (
            datetime.now(last_attempt.tzinfo)
            if last_attempt.tzinfo is not None
            else datetime.now()
        )
        return now - last_attempt > timedelta(seconds=self.processing_lock_ttl_seconds)

    async def _process_order(self, order: OrderDict, factory: OrderProcessorFactory) -> None:
        lock_acquired = False
        processing_error = None
        try:
            metadata = self._process_metadata(order.get("metadata"))
            order["metadata"] = metadata

            if metadata.get("processing_lock"):
                if self._is_processing_lock_stale(metadata):
                    logger.warning(
                        "Stale processing_lock released for order %s (ttl=%ss)",
                        order["id"],
                        self.processing_lock_ttl_seconds,
                    )
                    metadata["processing_lock"] = False
                    metadata["stale_lock_released_at"] = datetime.now().isoformat()
                else:
                    return

            metadata["processing_lock"] = True
            metadata["last_processing_attempt"] = datetime.now().isoformat()

            order_update = OrderUpdate(
                metadata=OrderProcessorUtils.sanitize_for_json(metadata)
            )
            await self.orders_repo.update_order(
                order_id=order["id"], order_data=order_update
            )
            lock_acquired = True

            processor = await factory.get_processor_for_order(order["id"])
            testing_enabled = config.get("testing.enabled", False)
            if testing_enabled:
                result = await processor.update_order_status(
                    order_id=order["id"], status=OrderStatus.COMPLETED
                )
            else:
                result = await processor.process_order(order["id"])

            if not isinstance(result, dict):
                logger.warning(
                    f"Unexpected result type from processor: {type(result)}"
                )
                result_info = str(result) if result is not None else "None"
                logger.info(
                    f"Processed order {order['id']} with non-dictionary result: {result_info}"
                )
            else:
                logger.info(
                    f"Processed order {order['id']} with result: {result}"
                )
        except Exception as e:
            processing_error = e
            logger.error(
                f"Error while processing order {order['id']}: {traceback.format_exc()}"
            )
        finally:
            if not lock_acquired:
                return

            try:
                current_order = await self.orders_repo.get_order_by_id(order["id"])
                if current_order:
                    current_metadata = self._process_metadata(
                        current_order.get("metadata")
                    )

                    if not current_metadata.get("processing_lock"):
                        return

                    current_metadata["processing_lock"] = False
                    if processing_error is None:
                        current_metadata["last_processed"] = datetime.now().isoformat()
                        current_metadata.pop("processing_error", None)
                        current_metadata.pop("last_error_time", None)
                    else:
                        current_metadata["processing_error"] = str(processing_error)
                        current_metadata["last_error_time"] = (
                            datetime.now().isoformat()
                        )

                    order_update = OrderUpdate(
                        metadata=OrderProcessorUtils.sanitize_for_json(
                            current_metadata
                        )
                    )
                    await self.orders_repo.update_order(
                        order_id=order["id"], order_data=order_update
                    )
            except Exception as unlock_error:
                logger.error(
                    f"Error releasing lock for order {order['id']}: {unlock_error}"
                )

    async def _process_orders(self, paid_orders):
        factory = OrderProcessorFactory(self.orders_repo, self.product_repo)
        max_parallel = max(1, int(self.max_parallel_orders))
        semaphore = asyncio.Semaphore(max_parallel)

        async def process_with_limit(order):
            async with semaphore:
                await self._process_order(order, factory)

        await asyncio.gather(*(process_with_limit(order) for order in paid_orders))

    async def process_paid_orders_other_games(self):
        try:
            categories = [
                ProductCategory.GCRYSTALS,
            ]
            limit = 20
            start = time.perf_counter()
            orders = []
            orders.extend(
                await self.orders_repo.get_orders_by_statuses(
                    statuses=[OrderStatus.PAID],
                    include_only=categories,
                    is_w_telegram_id=False,
                    limit=20
                )
            )
            if len(orders) < limit:
                processing_orders = await self.orders_repo.get_orders_by_statuses(
                    statuses=[OrderStatus.PROCESSING, OrderStatus.API_PENDING],
                    include_only=categories,
                    is_w_telegram_id=False,
                    limit=(limit-len(orders))
                )
                orders.extend(processing_orders)
            if not orders:
                end = time.perf_counter()
                elapsed = end - start
                logger.info(
                    "Time to execute periodic"
                    f" task process_paid_orders_other_games: {elapsed:.6f} секунд"
                )
                return

            await self._process_orders(orders)

            end = time.perf_counter()
            elapsed = end - start
            logger.info(
                "Time to execute periodic "
                f"task process_paid_orders_other_games: {elapsed:.6f} секунд"
            )

        except Exception as e:
            logger.error(f"Error while fetching orders_other_games: {e}")

    def start(self):
        self.orders_repo = OrdersRepository()
        self.product_repo = ProductRepository()
        self.orders_controller = OrdersController(orders_repository=self.orders_repo)
        try:
            ttl_seconds = config.get("order_processor.processing_lock_ttl_seconds", 600)
            self.processing_lock_ttl_seconds = max(1, int(ttl_seconds))
        except (TypeError, ValueError):
            logger.warning(
                "Invalid order_processor.processing_lock_ttl_seconds=%s, fallback to 600",
                config.get("order_processor.processing_lock_ttl_seconds"),
            )
            self.processing_lock_ttl_seconds = 600
        try:
            max_parallel = config.get("order_processor.max_parallel_orders", 5)
            self.max_parallel_orders = max(1, int(max_parallel))
        except (TypeError, ValueError):
            logger.warning(
                "Invalid order_processor.max_parallel_orders=%s, fallback to 5",
                config.get("order_processor.max_parallel_orders"),
            )
            self.max_parallel_orders = 5

        self.scheduler.add_job(
            self.process_paid_orders_other_games,
            IntervalTrigger(seconds=self.interval_seconds_tg),
            id="process_orders_other_games",
        )

        self.scheduler.start()

    def shutdown(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
