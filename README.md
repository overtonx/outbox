# Реализация паттерна Outbox

Этот проект представляет собой реализацию паттерна "Transactional Outbox" на Go. Он обеспечивает надежную асинхронную доставку сообщений из микросервисов в брокер сообщений (по умолчанию Kafka), даже в случае сбоев.

## Установка

```go
go get github.com/overtonx/outbox/v3
```

## Основной флоу

1.  **Сохранение события**: Вместо прямой отправки сообщения в брокер, сервис сохраняет его как событие (`Event`) в специальную таблицу `outbox_events` в своей базе данных. Это происходит в рамках той же транзакции, что и основная бизнес-логика. Это гарантирует, что событие будет сохранено только в том случае, если бизнес-транзакция успешно завершена.
2.  **Фоновая обработка**: Отдельный процесс, **Диспетчер (`Dispatcher`)**, периодически опрашивает таблицу `outbox_events` на наличие новых, необработанных событий.
3.  **Публикация**: Обнаружив новые события, `Dispatcher` с помощью **Публикатора (`Publisher`)** отправляет их в брокер сообщений.
4.  **Обновление статуса**: После успешной отправки `Dispatcher` помечает событие в таблице как обработанное. В случае сбоя отправки, он увеличивает счетчик попыток и планирует повторную отправку с использованием настраиваемой стратегии отсрочки (backoff).
5.  **Dead-Letter Queue**: Если событие не удается доставить после максимального количества попыток, оно перемещается в таблицу "мертвых писем" (`outbox_deadletters`) для последующего анализа.

## Компоненты

-   **`Outbox`**: Центральная точка входа. Создаётся через `New(db, serializer)` и предоставляет фабричные методы для `EventStore` и `Dispatcher`.
-   **`EventStore`**: Сохраняет события в таблицу `outbox_events`, сериализуя payload с помощью настроенного `Serializer`.
-   **`Serializer`**: Интерфейс для сериализации payload. Встроенный `JSONSerializer` сериализует данные в JSON. Можно реализовать свой `Serializer` для protobuf, Avro и других форматов.
-   **`Dispatcher`**: Ядро системы. Управляет воркерами, которые опрашивают базу данных, обрабатывают и публикуют события, а также выполняют очистку.
-   **`Publisher`**: Интерфейс для отправки сообщений. По умолчанию предоставляется `KafkaPublisher`. Вы можете реализовать свой собственный `Publisher` для интеграции с другими брокерами (например, RabbitMQ).
-   **Воркеры (`Worker`)**: Фоновые процессы, управляемые `Dispatcher`, для выполнения конкретных задач:
    -   `EventProcessor`: Обрабатывает и публикует новые события.
    -   `DeadLetterService`: Перемещает неисправимые события в DLQ.
    -   `StuckEventService`: Восстанавливает "зависшие" события, которые находились в обработке слишком долго.
    -   `CleanupService`: Удаляет старые обработанные события и записи из DLQ.

## Быстрый старт

```go
import "github.com/overtonx/outbox/v3"

// 1. Создание фасада с выбором сериализатора
ob := outbox.New(db, outbox.JSONSerializer{})

// 2. Сохранение события внутри бизнес-транзакции
tx, _ := db.BeginTx(ctx, nil)

store := ob.EventStore()
err := store.Save(ctx, tx, outbox.Event{
    EventType:     "order.created",
    AggregateType: "order",
    AggregateID:   "order-123",
    Topic:         "orders",
    Payload:       orderData,
})
if err != nil {
    tx.Rollback()
    return err
}
tx.Commit()

// 3. Запуск диспетчера в фоне
dispatcher, err := ob.Dispatcher(
    outbox.WithPublisher(kafkaPublisher),
    outbox.WithPollInterval(5 * time.Second),
)
if err != nil {
    return err
}
go dispatcher.Start(context.Background())
```

## Схема базы данных

```sql
CREATE TABLE outbox_events (
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_id       VARCHAR(36)  NOT NULL UNIQUE,
    event_type     VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    topic          VARCHAR(255) NOT NULL DEFAULT '',
    content_type   VARCHAR(100) NOT NULL DEFAULT 'application/json',
    payload        LONGBLOB     NOT NULL,
    headers        JSON         DEFAULT NULL,
    status         VARCHAR(50)  NOT NULL DEFAULT 'new',
    attempts       INT          NOT NULL DEFAULT 0,
    last_attempted_at DATETIME  DEFAULT NULL,
    next_attempt_at   DATETIME  DEFAULT NULL,
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE outbox_deadletters (
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_id       VARCHAR(36)  NOT NULL,
    event_type     VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    topic          VARCHAR(255) NOT NULL DEFAULT '',
    content_type   VARCHAR(100) NOT NULL DEFAULT 'application/json',
    payload        LONGBLOB     NOT NULL,
    headers        JSON         DEFAULT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_error     TEXT         DEFAULT NULL,
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

## Сериализация

`Outbox` использует интерфейс `Serializer` для преобразования payload перед записью в БД. Тип сериализации сохраняется в колонке `content_type` и передаётся потребителям Kafka через заголовок `content-type`.

### Доступные сериализаторы

| Пакет                                              | Тип                | `content_type`         | Описание                              |
|----------------------------------------------------|--------------------|------------------------|---------------------------------------|
| `github.com/overtonx/outbox/v3`                    | `JSONSerializer`   | `application/json`     | Встроен, используется по умолчанию    |
| `github.com/overtonx/outbox/v3/protoserializer`    | `ProtoSerializer`  | `application/protobuf` | Protobuf binary encoding              |

Константы `ContentTypeJSON` и `ContentTypeProtobuf` экспортируются из основного пакета для использования в потребителях.

### JSONSerializer

Встроен в основной пакет, не требует дополнительных зависимостей.

```go
ob := outbox.New(db, outbox.JSONSerializer{})
```

### ProtoSerializer

```go
go get github.com/overtonx/outbox/v3/protoserializer
```

```go
import (
    "github.com/overtonx/outbox/v3"
    "github.com/overtonx/outbox/v3/protoserializer"
)

ob := outbox.New(db, protoserializer.ProtoSerializer{})
store := ob.EventStore()
err := store.Save(ctx, tx, outbox.Event{
    EventType:     "order.created",
    AggregateType: "order",
    AggregateID:   "order-1",
    Topic:         "orders",
    Payload:       myProtoMessage, // proto.Message
})
```

Потребители Kafka получат заголовок `content-type: application/protobuf` и смогут выбрать нужный десериализатор.

### Свой сериализатор

Реализуйте интерфейс `Serializer` для любого другого формата (Avro, MessagePack и т.д.):

```go
type Serializer interface {
    Marshal(v interface{}) ([]byte, error)
    ContentType() string
}
```

## Конфигурация Диспетчера (`Dispatcher`)

```go
dispatcher, err := ob.Dispatcher(
    outbox.WithPollInterval(5 * time.Second),
    outbox.WithMaxAttempts(5),
    outbox.WithPublisher(myCustomPublisher),
)
if err != nil {
    // ...
}

go dispatcher.Start(context.Background())
```

### Опции `Dispatcher`:

-   `WithPollInterval(time.Duration)`: Интервал опроса таблицы `outbox_events` на наличие новых событий. (По умолчанию: 2 секунды)
-   `WithBatchSize(int)`: Максимальное количество событий, запрашиваемых из БД за один раз. Допустимый диапазон: 1–10000. (По умолчанию: 100)
-   `WithMaxAttempts(int)`: Максимальное количество попыток отправки события. (По умолчанию: 3)
-   `WithBackoffStrategy(BackoffStrategy)`: Стратегия вычисления задержки между повторными попытками.
-   `WithPublisher(Publisher)`: Позволяет указать собственную реализацию `Publisher`.
-   `WithKafkaConfig(KafkaConfig)`: Создаёт `KafkaPublisher` с переданной конфигурацией.
-   `WithLogger(*zap.Logger)`: Настройка логирования.
-   `WithMetrics(MetricsCollector)`: Подключение коллектора метрик (например, OpenTelemetry).
-   `WithStuckEventTimeout(time.Duration)`: Время, по истечении которого событие в статусе "в обработке" считается "зависшим". (По умолчанию: 10 минут)
-   `WithCleanupInterval(time.Duration)`: Интервал запуска воркера очистки. (По умолчанию: 1 час)
-   `WithSentEventsRetention(time.Duration)`: Как долго хранить успешно отправленные события. (По умолчанию: 24 часа)

## Конфигурация `KafkaPublisher`

По умолчанию используется `KafkaPublisher`. Его можно настроить с помощью `NewKafkaPublisherWithConfig` или опции `WithKafkaConfig`.

```go
kafkaConfig := outbox.DefaultKafkaConfig()
kafkaConfig.Topic = "my-default-topic"
kafkaConfig.ProducerProps["bootstrap.servers"] = "kafka1:9092,kafka2:9092"

publisher, err := outbox.NewKafkaPublisherWithConfig(logger, kafkaConfig)
if err != nil {
    // ...
}

dispatcher, err := ob.Dispatcher(outbox.WithPublisher(publisher))
```

### Опции `KafkaConfig`:

-   `Topic (string)`: Имя топика по умолчанию, которое будет использоваться, если топик не указан в самом событии.
-   `ProducerProps (kafka.ConfigMap)`: Карта для настройки нативного Kafka-продюсера из `confluent-kafka-go`. Позволяет задавать любые параметры, такие как `bootstrap.servers`, `acks`, `compression.type` и т.д.
-   `HeaderBuilder (KafkaHeaderBuilder)`: Функция для создания заголовков Kafka-сообщения.

### Заголовки Kafka

По умолчанию каждое сообщение получает следующие заголовки: `event_id`, `event_type`, `aggregate_type`, `aggregate_id`, `content-type`. Дополнительные заголовки можно передать через поле `Event.Headers` (JSON-объект).

Системные заголовки (`event_id`, `event_type`, `aggregate_type`, `aggregate_id`, `content-type`) защищены от переопределения пользовательскими данными.

**Пример пользовательского конструктора заголовков:**

```go
func myCustomHeaderBuilder(record outbox.EventRecord) []kafka.Header {
    headers := outbox.BuildKafkaHeaders(record)
    headers = append(headers, kafka.Header{
        Key:   "X-Custom-Header",
        Value: []byte("my-value"),
    })
    return headers
}

kafkaConfig := outbox.KafkaConfig{
    HeaderBuilder: myCustomHeaderBuilder,
}
```

## Публикация сообщений и выбор топика

1.  **Приоритет у события**: Если при создании события указан конкретный топик, сообщение будет отправлено именно в него.

    ```go
    err := store.Save(ctx, tx, outbox.Event{
        Topic:   "user-events",
        Payload: data,
        // ...
    })
    ```

2.  **Топик по умолчанию**: Если поле `Topic` пустое, используется топик из `KafkaConfig.Topic`.

## Миграция с v2 на v3

### Установка

```go
go get github.com/overtonx/outbox/v3
```

Замените импорты:

```go
// было
import "github.com/overtonx/outbox/v2"

// стало
import "github.com/overtonx/outbox/v3"
```

### Изменения схемы БД

Колонка `payload` переведена из `JSON` в `LONGBLOB` для поддержки бинарных форматов. Добавлена колонка `content_type`.

```sql
ALTER TABLE outbox_events
    MODIFY COLUMN payload LONGBLOB NOT NULL,
    ADD COLUMN content_type VARCHAR(100) NOT NULL DEFAULT 'application/json' AFTER topic;

ALTER TABLE outbox_deadletters
    MODIFY COLUMN payload LONGBLOB NOT NULL,
    ADD COLUMN content_type VARCHAR(100) NOT NULL DEFAULT 'application/json' AFTER topic;
```

> **Важно:** выполните миграцию в обслуживающем окне. После изменения типа `payload` откат потребует повторного преобразования данных.

### Изменения API

В v3 появился фасад `Outbox` как единая точка входа, а `SaveEvent` помечена как устаревшая.

**Было (v2):**

```go
err := outbox.SaveEvent(ctx, tx, event)

dispatcher, err := outbox.NewDispatcher(db, outbox.WithLogger(logger))
```

**Стало (v3):**

```go
ob := outbox.New(db, outbox.JSONSerializer{})

store := ob.EventStore()
err := store.Save(ctx, tx, event)

dispatcher, err := ob.Dispatcher(outbox.WithLogger(logger))
```

`SaveEvent` продолжает работать через делегирование к `EventStore` с `JSONSerializer` — ломающих изменений нет, но функция будет удалена в v4.
