# UC Shop Order Pipeline

Рабочая event-driven обработка заказов через Postgres transactional outbox, Kafka, replicas consumer group, retry/DLQ и идемпотентность.

## Что Важно

- API не пишет напрямую в Kafka. Он пишет `orders` и `order_events_outbox` в одной Postgres transaction.
- `outbox-relay` публикует due events из БД в Kafka и безопасно масштабируется через `FOR UPDATE SKIP LOCKED`.
- `order-processor` читает `orders.paid` и retry topics в одном consumer group.
- Перед вызовом провайдера processor делает atomic claim заказа в БД.
- Retry не блокирует Kafka consumer: retry event лежит в outbox до `publish_after`.
- Горизонтальное масштабирование делается через replicas в Docker Compose и Kafka partitions.

## Архитектура

```text
order-api replicas
  -> Postgres orders + order_events_outbox
  -> outbox-relay replicas
  -> Kafka: orders.paid / orders.retry.* / orders.dlq
  -> order-processor replicas
  -> provider API
  -> Postgres order status
```

## Структура

```text
src/order_pipeline/
  domain/            чистые сущности, события, ошибки
  application/       use cases, ports, retry policy
  infrastructure/    Postgres, Kafka, provider adapters
  platform/          config, ids, logging
  app/               runnable API, consumer, outbox services
```

Это Clean/Hexagonal стиль: домен и use cases не импортируют Kafka/FastAPI/Postgres. Файлы нарезаны по single responsibility и держатся до 100 строк.

## Локальный Запуск

```bash
docker compose up --build --scale outbox-relay=2 --scale order-processor=3
```

Создать заказ:

```bash
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1' \
  -d '{"category":"GCRYSTALS","telegram_id":"123","payload":{"amount":100}}'
```

Проверить:

```bash
curl http://localhost:8000/orders
```

Проверить retry:

```bash
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: retry-order-1' \
  -d '{"category":"GCRYSTALS","payload":{"force_fail":true}}'
```

## Масштабирование

```bash
ORDER_API_WORKERS=6 docker compose up --build -d \
  --scale outbox-relay=2 \
  --scale order-processor=6
```

- `order-api` масштабируется количеством ASGI workers.
- `outbox-relay` масштабируется replicas и безопасно делит строки через `FOR UPDATE SKIP LOCKED`.
- `order-processor` масштабируется replicas внутри одного Kafka consumer group.
- Kafka topics в compose имеют 96 partitions и `replication.factor=3`.
- Postgres pool настраивается через `POSTGRES_POOL_MAX_SIZE`.

## Load Testing

Нагрузочный инструмент лежит в `tools/load`.

Проверенный локальный профиль:

- 3 Kafka brokers, RF=3, `min.insync.replicas=2`;
- 6 API workers;
- 6 processor replicas;
- 2 outbox relay replicas;
- 500 rps sustained: 5000 заказов, 0 ошибок, все дошли до `COMPLETED`;
- 1000 rps burst: 10000 заказов, 0 ошибок, все дошли до `COMPLETED`;
- broker failover: при остановке `kafka-2` обработка продолжила работать на ISR=2.

Kafka в compose поднята как 3-node KRaft cluster. Retry сделан через отдельные topics:
`orders.retry.1s`, `orders.retry.3s`, `orders.retry.5s`.

## Test Scripts

Скрипты для демонстрации и проверки лежат в `scripts`.

```bash
./scripts/monitor.py
./scripts/smoke_test.sh
./scripts/retry_test.sh
RPS=500 DURATION=10 CONCURRENCY=1200 ./scripts/load_test.sh
./scripts/failover_test.sh
./scripts/full_check.sh
```

## Инварианты Надежности

- `acks=all` и idempotent Kafka producer.
- `replication.factor=3`, `min.insync.replicas=2`.
- Любой внешний provider call получает стабильный `idempotency_key`.
- Kafka offset commit происходит только после завершения обработки batch.
- Повторная доставка Kafka безопасна: atomic claim не даст выдать один заказ дважды.
- DLQ сохраняет проблемные события для ручного разбора.
