# Реализация паттерна Outbox

Этот проект представляет собой реализацию паттерна "Transactional Outbox" на Go. Он обеспечивает надежную асинхронную доставку сообщений из микросервисов в брокер сообщений (по умолчанию Kafka), даже в случае сбоев.

## Установка

```bash
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
-   **`EventStore`**: Сохраняет события в таблицу `outbox_events`, сериализуя payload с помощью настроенного `Serializer`. Поддерживает опциональный `EventMapper` для преобразования события перед сохранением (`WithMapper`).
-   **`Serializer`** (`github.com/overtonx/outbox/v3/serializer`): Интерфейс для сериализации payload. Встроены `JSONSerializer` и `ProtoSerializer`. Можно реализовать свой для Avro, MessagePack и других форматов.
-   **`Dispatcher`**: Ядро системы. Управляет воркерами, которые опрашивают базу данных, обрабатывают и публикуют события, а также выполняют очистку.
-   **`Publisher`**: Интерфейс для отправки сообщений. По умолчанию предоставляется `KafkaPublisher`. Вы можете реализовать свой собственный `Publisher` для интеграции с другими брокерами (например, RabbitMQ).
-   **Воркеры (`Worker`)**: Фоновые процессы, управляемые `Dispatcher`:
    -   `EventProcessor`: Обрабатывает и публикует новые события.
    -   `DeadLetterService`: Перемещает неисправимые события в DLQ.
    -   `StuckEventService`: Восстанавливает "зависшие" события.
    -   `CleanupService`: Удаляет старые обработанные события и записи из DLQ.

## Быстрый старт

```go
import (
    "github.com/overtonx/outbox/v3"
    "github.com/overtonx/outbox/v3/serializer"
)

// 1. Создание фасада с выбором сериализатора
ob := outbox.New(db, serializer.JSONSerializer{})

// 2. Сохранение события внутри бизнес-транзакции
tx, _ := db.BeginTx(ctx, nil)

store := ob.EventStore()
err := store.SaveWithDB(ctx, tx, outbox.Event{
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

Таблицы создаются автоматически при инициализации `Dispatcher`. Актуальная схема:

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id        CHAR(36)     NOT NULL UNIQUE,
    event_type      VARCHAR(255) NOT NULL,
    aggregate_type  VARCHAR(255) NOT NULL,
    aggregate_id    VARCHAR(255) NOT NULL,
    status          INT          NOT NULL DEFAULT 0, -- 0=new, 1=sent, 2=retry, 3=error, 4=processing
    topic           VARCHAR(255) NOT NULL,
    content_type    VARCHAR(100) NOT NULL DEFAULT 'application/json',
    payload         LONGBLOB     NOT NULL,
    headers         JSON         NULL,
    attempt_count   INT          NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP    NULL,
    last_error      TEXT         NULL,
    created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS outbox_deadletters (
    id             BIGINT PRIMARY KEY,
    event_id       CHAR(36)      NOT NULL UNIQUE,
    event_type     VARCHAR(255)  NOT NULL,
    aggregate_type VARCHAR(255)  NOT NULL,
    aggregate_id   VARCHAR(255)  NOT NULL,
    topic          VARCHAR(255)  NOT NULL,
    content_type   VARCHAR(100)  NOT NULL DEFAULT 'application/json',
    payload        LONGBLOB      NOT NULL,
    headers        JSON          NULL,
    attempt_count  INT           NOT NULL,
    last_error     VARCHAR(2000) NULL,
    created_at     TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Сериализация

`Outbox` использует интерфейс `serializer.Serializer` для преобразования payload перед записью в БД. Тип сериализации сохраняется в колонке `content_type` и передаётся потребителям Kafka через заголовок `content-type`.

### Доступные сериализаторы

Пакет: `github.com/overtonx/outbox/v3/serializer`

| Тип                | `content_type`         | Описание                        |
|--------------------|------------------------|---------------------------------|
| `JSONSerializer`   | `application/json`     | JSON-кодирование (по умолчанию) |
| `ProtoSerializer`  | `application/protobuf` | Protobuf binary encoding        |

### JSONSerializer

```go
import "github.com/overtonx/outbox/v3/serializer"

ob := outbox.New(db, serializer.JSONSerializer{})
```

### ProtoSerializer

Payload должен реализовывать `proto.Message`.

```go
import "github.com/overtonx/outbox/v3/serializer"

ob := outbox.New(db, serializer.ProtoSerializer{})
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

Реализуйте интерфейс `serializer.Serializer` для любого другого формата:

```go
import "github.com/overtonx/outbox/v3/serializer"

type AvroSerializer struct{ schema string }

func (s AvroSerializer) Marshal(v interface{}) ([]byte, error) {
    // avro-кодирование
}

func (s AvroSerializer) ContentType() string { return "application/avro" }

// Использование
ob := outbox.New(db, AvroSerializer{schema: "..."})
```

## Маппер событий (`EventMapper`)

`EventMapper` — опциональный хук, который позволяет преобразовать `Event` непосредственно перед его сериализацией и записью в БД. Удобен для обогащения заголовков, нормализации полей или добавления сквозных метаданных без изменения бизнес-кода.

Маппер вызывается **после** инъекции трассировочного контекста и **до** сериализации payload.

```go
// EventMapper — это функция вида func(Event) Event.
store := outbox.NewEventStore(serializer.JSONSerializer{}).
    WithMapper(func(e outbox.Event) outbox.Event {
        if e.Headers == nil {
            e.Headers = make(map[string]string)
        }
        e.Headers["x-source"] = "payment-service"
        e.Headers["x-env"]    = os.Getenv("APP_ENV")
        return e
    })
```

Через фасад `Outbox`:

```go
ob := outbox.New(db, serializer.JSONSerializer{})
store := ob.EventStore().WithMapper(func(e outbox.Event) outbox.Event {
    e.EventType = strings.ToLower(e.EventType)
    return e
})
```

`WithMapper` возвращает тот же `*EventStore`, поэтому вызовы можно цепочкой.
Если маппер не задан, поведение не изменяется.

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

-   `WithPollInterval(time.Duration)`: Интервал опроса таблицы `outbox_events`. (По умолчанию: 2 секунды)
-   `WithBatchSize(int)`: Количество событий за один запрос. Диапазон: 1–10000. (По умолчанию: 100)
-   `WithMaxAttempts(int)`: Максимальное количество попыток отправки. (По умолчанию: 3)
-   `WithBackoffStrategy(BackoffStrategy)`: Стратегия задержки между повторными попытками.
-   `WithPublisher(Publisher)`: Собственная реализация `Publisher`.
-   `WithKafkaConfig(KafkaConfig)`: Создаёт `KafkaPublisher` с переданной конфигурацией.
-   `WithLogger(*zap.Logger)`: Настройка логирования.
-   `WithMetrics(MetricsCollector)`: Подключение коллектора метрик (например, OpenTelemetry).
-   `WithStuckEventTimeout(time.Duration)`: Время, после которого событие в статусе "в обработке" считается зависшим. (По умолчанию: 10 минут)
-   `WithCleanupInterval(time.Duration)`: Интервал запуска воркера очистки. (По умолчанию: 1 час)
-   `WithSentEventsRetention(time.Duration)`: Время хранения успешно отправленных событий. (По умолчанию: 24 часа)

## Конфигурация `KafkaPublisher`

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

-   `Topic`: Топик по умолчанию (используется, если топик не задан в событии).
-   `ProducerProps`: Параметры нативного Kafka-продюсера (`confluent-kafka-go`): `bootstrap.servers`, `acks`, `compression.type` и т.д.
-   `HeaderBuilder`: Функция для создания заголовков Kafka-сообщения.

### Заголовки Kafka

Каждое сообщение автоматически получает заголовки: `event_id`, `event_type`, `aggregate_type`, `aggregate_id`, `content-type`. Дополнительные заголовки передаются через `Event.Headers`.

Системные заголовки защищены от переопределения пользовательскими данными.

**Пример пользовательского `HeaderBuilder`:**

```go
func myHeaderBuilder(record outbox.EventRecord) []kafka.Header {
    headers := outbox.BuildKafkaHeaders(record)
    headers = append(headers, kafka.Header{
        Key:   "X-Custom-Header",
        Value: []byte("my-value"),
    })
    return headers
}

kafkaConfig := outbox.KafkaConfig{
    HeaderBuilder: myHeaderBuilder,
}
```

## Публикация сообщений и выбор топика

1.  **Приоритет у события**: если `Topic` задан в событии, сообщение идёт в него.

    ```go
    store.Save(ctx, tx, outbox.Event{Topic: "user-events", Payload: data, ...})
    ```

2.  **Топик по умолчанию**: если `Topic` пустой, используется `KafkaConfig.Topic`.

## Миграция с v2 на v3

### Установка

```bash
go get github.com/overtonx/outbox/v3
```

Замените импорты:

```go
// было
import "github.com/overtonx/outbox/v2"

// стало
import (
    "github.com/overtonx/outbox/v3"
    "github.com/overtonx/outbox/v3/serializer"
)
```

### Изменения схемы БД

Колонка `payload` переведена из `JSON` в `LONGBLOB`. Добавлена колонка `content_type`.

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

**Было (v2):**

```go
err := outbox.SaveEvent(ctx, tx, event)

dispatcher, err := outbox.NewDispatcher(db, outbox.WithLogger(logger))
```

**Стало (v3):**

```go
ob := outbox.New(db, serializer.JSONSerializer{})

store := ob.EventStore()
err := store.SaveWithDB(ctx, tx, event)

dispatcher, err := ob.Dispatcher(outbox.WithLogger(logger))
```

`SaveEvent` продолжает работать — ломающих изменений нет, но функция будет удалена в v4.
