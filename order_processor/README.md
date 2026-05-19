### Запуск

```bash
cd order_processor
docker-compose up -d --build
```

### Проверка работы (можно запустить для любого воркера)

```bash
docker exec -it order_processor-worker-1 python3 test_producer.py
```
```bash
docker exec -it order_processor-worker-1 python3 test_duplicate_producer.py
```

### Остановка

```bash
docker-compose down
```

### Суть решения

```
Заказ создается -> Redis Stream (orders) -> Consumer Group (order_processors) -> Workers
                                                    |
                                                    v
                              XACK / Retry (без ack при ошибке) / DLQ (orders_dlq)
```

### Преимущества использования Redis Streams

**Event-driven**: Заказы обрабатываются сразу после создания, без polling

**Distributed**: Несколько consumer'ов в одной группе распределяют сообщения между собой автоматически

**Persistent**: Сообщения хранятся в Redis до явного подтверждения обработки (XACK) или истечения retention

**Scalable**: Масштабируется за счет увеличения количества worker'ов в одной consumer group

**Proper ordering**: Сообщения внутри одного stream обрабатываются последовательно, а consumer group гарантирует распределение между воркерами

**DLQ**: Сообщения, исчерпавшие все retry, отправляются в отдельный stream `orders_dlq` для ручного разбора

**FSM**: Переходы статусов заказов валидируются через Finite State Machine — невалидные переходы (например, COMPLETED -> PENDING) отклоняются
