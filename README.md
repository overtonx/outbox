# Реализация паттерна Outbox

Этот проект представляет собой реализацию паттерна "Transactional Outbox" на Go. Он обеспечивает надежную асинхронную доставку сообщений из микросервисов в брокер сообщений (по умолчанию Kafka), даже в случае сбоев.

## Установка

Для установки пакета выполните команду:

```go
go get github.com/overtonx/outbox/v2
```

## Основной флоу
 
1.  **Сохранение события**: Вместо прямой отправки сообщения в брокер, сервис сохраняет его как событие (`Event`) в специальную таблицу `outbox_events` в своей базе данных. Это происходит в рамках той же транзакции, что и основная бизнес-логика. Это гарантирует, что событие будет сохранено только в том случае, если бизнес-транзакция успешно завершена.
2.  **Фоновая обработка**: Отдельный процесс, **Диспетчер (`Dispatcher`)**, периодически опрашивает таблицу `outbox_events` на наличие новых, необработанных событий.
3.  **Публикация**: Обнаружив новые события, `Dispatcher` с помощью **Публикатора (`Publisher`)** отправляет их в брокер сообщений.
4.  **Обновление статуса**: После успешной отправки `Dispatcher` помечает событие в таблице как обработанное. В случае сбоя отправки, он увеличивает счетчик попыток и планирует повторную отправку с использованием настраиваемой стратегии отсрочки (backoff).
5.  **Dead-Letter Queue**: Если событие не удается доставить после максимального количества попыток, оно перемещается в таблицу "мертвых писем" (`outbox_deadletters`) для последующего анализа.

## Компоненты

-   **`outbox`**: Основной пакет для создания и сохранения событий (`SaveEvent`).
-   **`Dispatcher`**: Ядро системы. Управляет воркерами, которые опрашивают базу данных, обрабатывают и публикуют события, а также выполняют очистку.
-   **`Publisher`**: Интерфейс для отправки сообщений. По умолчанию предоставляется `KafkaPublisher`. Вы можете реализовать свой собственный `Publisher` для интеграции с другими брокерами (например, RabbitMQ).
-   **Воркеры (`Worker`)**: Фоновые процессы, управляемые `Dispatcher`, для выполнения конкретных задач:
    -   `EventProcessor`: Обрабатывает и публикует новые события.
    -   `DeadLetterService`: Перемещает неисправимые события в DLQ.
    -   `StuckEventService`: Восстанавливает "зависшие" события, которые находились в обработке слишком долго.
    -   `CleanupService`: Удаляет старые обработанные события и записи из DLQ.

## Конфигурация Диспетчера (`Dispatcher`)

Диспетчер создается с помощью `NewDispatcher` и настраивается через функциональные опции (`DispatcherOption`).

```go
// Пример создания с опциями
dispatcher, err := outbox.NewDispatcher(
    db, // *sql.DB
    outbox.WithPollInterval(5 * time.Second),
    outbox.WithMaxAttempts(5),
    outbox.WithPublisher(myCustomPublisher),
)
if err != nil {
    // ...
}

// Запуск диспетчера
go dispatcher.Start(context.Background())
```

### Основные опции `Dispatcher`:

-   `WithPollInterval(time.Duration)`: Интервал опроса таблицы `outbox_events` на наличие новых событий. (По умолчанию: 2 секунды)
-   `WithBatchSize(int)`: Максимальное количество событий, запрашиваемых из БД за один раз. (По умолчанию: 100)
-   `WithMaxAttempts(int)`: Максимальное количество попыток отправки события. (По умолчанию: 3)
-   `WithBackoffStrategy(BackoffStrategy)`: Стратегия вычисления задержки между повторными попытками.
-   `WithPublisher(Publisher)`: Позволяет указать собственную реализацию `Publisher`.
-   `WithLogger(*zap.Logger)`: Настройка логирования.
-   `WithStuckEventTimeout(time.Duration)`: Время, по истечении которого событие в статусе "в обработке" считается "зависшим". (По умолчанию: 10 минут)
-   `WithCleanupInterval(time.Duration)`: Интервал запуска воркера очистки. (По умолчанию: 1 час)
-   `WithSentEventsRetention(time.Duration)`: Как долго хранить успешно отправленные события. (По умолчанию: 24 часа)

## Конфигурация `Publisher`

По умолчанию используется `KafkaPublisher`. Его можно тонко настроить с помощью `NewKafkaPublisherWithConfig`.

```go
kafkaConfig := outbox.DefaultKafkaConfig()
kafkaConfig.Topic = "my-default-topic"
kafkaConfig.ProducerProps["bootstrap.servers"] = "kafka1:9092,kafka2:9092"

publisher, err := outbox.NewKafkaPublisherWithConfig(logger, kafkaConfig)
if err != nil {
    // ...
}

// Передача настроенного публикатора в диспетчер
dispatcher, err := outbox.NewDispatcher(db, outbox.WithPublisher(publisher))
```

### Опции `KafkaConfig`:

-   `Topic (string)`: Имя топика по умолчанию, которое будет использоваться, если топик не указан в самом событии.
-   `ProducerProps (kafka.ConfigMap)`: Карта для настройки нативного Kafka-продюсера из `confluent-kafka-go`. Позволяет задавать любые параметры, такие как `bootstrap.servers`, `acks`, `compression.type` и т.д.
-   `HeaderBuilder (KafkaHeaderBuilder)`: Функция для создания заголовков Kafka-сообщения.

### Зачем нужен `Headers Builder`?

`Headers Builder` (`KafkaHeaderBuilder`) — это функция, которая преобразует метаданные события (`event_id`, `event_type`, `aggregate_id`, `trace_id` и т.д.) в нативные заголовки Kafka-сообщения.

**Пример пользовательского конструктора заголовков:**

```go
func myCustomHeaderBuilder(record outbox.EventRecord) []kafka.Header {
    // Начинаем с заголовков по умолчанию
    headers := outbox.BuildKafkaHeaders(record)

    // Добавляем пользовательский заголовок
    headers = append(headers, kafka.Header{
        Key: "X-Custom-Header",
        Value: []byte("my-value"),
    })

    return headers
}

// Затем назначаем его в конфигурации
kafkaConfig := outbox.KafkaConfig{
    // ...
    HeaderBuilder: myCustomHeaderBuilder,
}
```

## Публикация сообщений и выбор топика

Логика выбора топика для публикации сообщения следующая:

1.  **Приоритет у события**: Если при создании события (`NewOutboxEvent`) вы указали конкретный топик, сообщение будет отправлено именно в него.

    ```go
    // Сообщение будет отправлено в топик "user-events"
    event, _ := NewOutboxEvent(..., "user-events", payload)
    SaveEvent(ctx, tx, event)
    ```

2.  **Топик по умолчанию**: Если при создании события поле `Topic` осталось пустым, будет использован топик по умолчанию, заданный в `KafkaConfig.Topic` при конфигурации `KafkaPublisher`.

    ```go
    // Topic не указан, будет использован топик из KafkaConfig
    event, _ := NewOutboxEvent(..., "", payload)
    SaveEvent(ctx, tx, event)
    ```

Такой подход обеспечивает гибкость: вы можете как направлять все события в один общий топик, так и маршрутизировать их по разным топикам в зависимости от бизнес-логики.

## Миграция с v1 на v2

В версии v2 изменилась схема таблицы `outbox_events` и `outbox_deadletters`. Поля `trace_id` и `span_id` были заменены одним универсальным полем `headers` типа `JSON`.

```sql
ALTER TABLE outbox_events ADD COLUMN headers JSON DEFAULT NULL AFTER payload;
ALTER TABLE outbox_deadletters ADD COLUMN headers JSON DEFAULT NULL AFTER payload;

ALTER TABLE outbox_events DROP COLUMN trace_id, DROP COLUMN span_id;
ALTER TABLE outbox_deadletters DROP COLUMN trace_id, DROP COLUMN span_id;
```

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

В v3 колонка `payload` переведена из `JSON` в `LONGBLOB` для поддержки бинарных форматов (protobuf, Avro и др.). Добавлена колонка `content_type` для указания формата сериализации.

```sql
-- outbox_events
ALTER TABLE outbox_events
    MODIFY COLUMN payload LONGBLOB NOT NULL,
    ADD COLUMN content_type VARCHAR(100) NOT NULL DEFAULT 'application/json' AFTER topic;

-- outbox_deadletters
ALTER TABLE outbox_deadletters
    MODIFY COLUMN payload LONGBLOB NOT NULL,
    ADD COLUMN content_type VARCHAR(100) NOT NULL DEFAULT 'application/json' AFTER topic;
```

> **Важно:** выполните миграцию в обслуживающем окне. После изменения типа `payload` откат потребует повторного преобразования данных.

### Изменения API

В v3 появился фасад `Outbox` как единая точка входа, а `SaveEvent` помечена как устаревшая.

**Было (v2):**

```go
// Сохранение события
err := outbox.SaveEvent(ctx, tx, event)

// Создание диспетчера
dispatcher, err := outbox.NewDispatcher(db, outbox.WithLogger(logger))
```

**Стало (v3):**

```go
// Создание фасада с выбором сериализатора
ob := outbox.New(db, outbox.JSONSerializer{})

// Сохранение события через EventStore
store := ob.EventStore()
err := store.Save(ctx, tx, event)

// Создание диспетчера через фасад
dispatcher, err := ob.Dispatcher(outbox.WithLogger(logger))
```

`SaveEvent` продолжает работать через делегирование к `EventStore` с `JSONSerializer` — ломающих изменений для существующего кода нет, но функция будет удалена в v4.

### Поддержка protobuf

Реализуйте интерфейс `Serializer` в своём сервисе:

```go
type ProtoSerializer struct{}

func (ProtoSerializer) Marshal(v interface{}) ([]byte, error) {
    msg, ok := v.(proto.Message)
    if !ok {
        return nil, fmt.Errorf("expected proto.Message, got %T", v)
    }
    return proto.Marshal(msg)
}

func (ProtoSerializer) ContentType() string { return outbox.ContentTypeProtobuf }
```

```go
ob := outbox.New(db, ProtoSerializer{})
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
