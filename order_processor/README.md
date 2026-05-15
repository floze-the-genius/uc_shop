### Запуск

```bash
cd order_processor
docker-compose up -d --build
```

### Проверка работы (можно запустить для любого воркера)

```bash
docker exec -it order_processor-worker-1 python3 test_producer.py
```

### Остановка

```bash
docker-compose down
```

### Суть решения

```
Заказ создается -> Kafka Topic (orders) -> Consumer Group (order_processors) -> Workers
                                                    |
                                                    v
                              Offset Commit / Retry (без коммита при ошибке)
```

### Преимущества использования Kafka

**Event-driven**: Заказы обрабатываются сразу после создания, без polling

**Distributed**: Несколько consumer'ов в одной group распределяют партиции между собой

**Persistent**: Сообщения хранятся на диске с настраиваемым retention

**Scalable**: Масштабируется за счет увеличения партиций и worker'ов

**Exactly-once / At-least-once**: Ручной offset commit после успешной обработки

**Proper ordering**: Сообщения внутри одной партиции обрабатываются последовательно
