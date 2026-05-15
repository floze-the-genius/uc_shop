### Запуск

```bash
cd order_processor
docker-compose up -d --build
```

### Проверка работы (можно запустить для любого воркера)

```bash
docker exec -it order_processor_worker_1 python3 test_producer.py
```

### Суть решения

```
Заказ создается -> Redis Stream -> Consumer Group -> Workers
                                      |
                                      v
                              Pending/ACK/Dead Letter
```

### Преимущества использования Redis Streams

**Event-driven**: Заказы обрабатываются сразу после создания, без polling

**Distributed**: Несколько consumer'ов могут обрабатывать один stream

**Built-in ACK**: Redis гарантирует доставку сообщений

**Automatic retries**: Необработанные сообщения возвращаются в очередь

**Scaling**: Легко добавить больше worker'ов

**Proper locking**: Consumer group обеспечивает exclusive processing