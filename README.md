# Реализация паттерна Outbox

Этот проект — реализация паттерна "Transactional Outbox" на Go для MySQL и Kafka.
Он обеспечивает надёжную асинхронную доставку сообщений из микросервисов в Kafka,
даже в случае сбоев, **сохраняя порядок публикации в пределах одного aggregate_id**
и оставаясь безопасным при запуске нескольких инстансов одного сервиса.

## Установка

```bash
go get github.com/overtonx/outbox/v4
```

## Основной флоу

1.  **Сохранение события**: вместо прямой отправки сообщения в Kafka сервис
    сохраняет его как событие (`Event`) в таблицу `outbox_events` — в рамках той
    же транзакции, что и основная бизнес-логика. Событие сохранится, только если
    бизнес-транзакция успешно закоммитится.
2.  **Единственный активный лидер**: `Dispatcher` можно поднимать в нескольких
    инстансах (несколько подов одного сервиса) — активно вычитывает и публикует
    события ровно один из них (лидер), выбранный через именованный advisory-лок
    MySQL (`GET_LOCK`). Остальные простаивают как hot standby и подхватывают
    публикацию, если лидер падает — обычно в течение одного `PollInterval`.
3.  **Клейминг и публикация в порядке вставки**: лидер выбирает события из БД,
    отсортированные по `created_at, id`, клеймит их (`SELECT ... FOR UPDATE SKIP
    LOCKED`) и публикует в Kafka строго в этом порядке. Тот же запрос
    восстанавливает события, зависшие в статусе "в обработке" после падения
    процесса — отдельного воркера восстановления не требуется.
4.  **Ретраи**: при ошибке публикации событие переходит в статус `retry` со
    временем следующей попытки по настраиваемой стратегии backoff, либо в
    `error`, если попытки исчерпаны.
5.  **Dead-Letter**: события, исчерпавшие попытки, переносятся в
    `outbox_deadletters` в рамках того же цикла лидера.

## Компоненты

-   **`Outbox`**: точка входа. Создаётся через `New(db, serializer)`, даёт
    фабричные методы для `EventStore` и `Dispatcher`.
-   **`EventStore`**: сохраняет события в `outbox_events`, сериализуя payload
    настроенным `Serializer`. `Save(ctx, event)` берёт исполнителя (`*sql.Tx`
    или `*sql.DB`) из контекста через `avito-tech/go-transaction-manager`;
    `SaveWithDB(ctx, exec, event)` принимает исполнителя явно. Поддерживает
    `EventMapper` (`WithMapper`) для преобразования события перед сохранением.
-   **`Serializer`** (`github.com/overtonx/outbox/v4/serializer`): `JSONSerializer`
    и `ProtoSerializer` из коробки, либо реализуйте свой.
-   **`Dispatcher`**: единственный компонент, отвечающий за выбор лидера
    (`GET_LOCK`) и за reconcile-цикл (claim → publish → dead-letter) на каждом
    `PollInterval`, пока процесс лидер.
-   **`Publisher`**: асинхронный интерфейс отправки сообщений — `Publish`
    возвращает ошибку только при сбое постановки в очередь, а `onDelivery`
    вызывается позже при получении отчёта о доставке. По умолчанию —
    `KafkaPublisher` с `enable.idempotence=true` и
    `max.in.flight.requests.per.connection=5`.
-   **`migrations`** (`github.com/overtonx/outbox/v4/migrations`): версионированная
    SQL-схема, встроенная через `go:embed`. Применяется автоматически при
    создании `Dispatcher`, либо используйте `FS()` со своим инструментом
    миграций (golang-migrate, goose и т.п.).

## Быстрый старт

```go
import (
    "github.com/overtonx/outbox/v4"
    "github.com/overtonx/outbox/v4/serializer"
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

// 3. Запуск диспетчера в фоне. Dispatcher.Start применяет миграции схемы
// при создании и блокируется до отмены ctx или Stop() — можно безопасно
// поднимать несколько инстансов против одной БД, лидер выберется сам.
dispatcher, err := ob.Dispatcher(
    outbox.WithPublisher(kafkaPublisher),
    outbox.WithPollInterval(5 * time.Second),
)
if err != nil {
    return err
}
go dispatcher.Start(context.Background())
```

## Гарантия порядка при нескольких инстансах

`Dispatcher` безопасно запускать в нескольких копиях одного сервиса против
одной БД — например, несколько подов Kubernetes. Активно клеймит и публикует
события **ровно один** инстанс (лидер): он получает именованный advisory-лок
MySQL (`SELECT GET_LOCK(name, 0)`), опрашиваемый каждым инстансом раз в
`PollInterval`. Имя лока по умолчанию выводится из имени текущей схемы БД
(`outbox:leader:<schema>`) — задайте его явно через `WithLockName`, если в
одной схеме работает несколько независимых outbox-каналов.

Пока инстанс держит лок, он выполняет reconcile-цикл (claim → publish →
dead-letter) каждый `PollInterval`. Если процесс-лидер падает или теряет
соединение с БД, лок освобождается автоматически (сессионный), и любой
простаивающий инстанс подхватывает лидерство на следующем цикле опроса —
обычно в пределах одного `PollInterval`.

Важно понимать границы гарантии:

-   Kafka сама не гарантирует порядок между партициями. Сообщения ключуются
    `AggregateID`, поэтому единственная гарантия, наблюдаемая потребителем —
    **порядок в пределах одного `aggregate_id`**. Единственный активный лидер
    даёт более сильную гарантию (полный порядок публикации внутри
    `Dispatcher`), чем шардинг по `aggregate_id` между инстансами.
-   Если процесс падает **после** успешной доставки в Kafka, но **до** записи
    статуса `sent`, событие будет переклеймлено и опубликовано повторно при
    восстановлении. Это стандартная at-least-once семантика outbox —
    потребители обязаны дедуплицировать по заголовку Kafka `event_id`.

### Пайплайн публикации и асинхронные отчёты о доставке

Лидер не ждёт подтверждения доставки каждого события по отдельности —
`Publisher.Publish` асинхронный: возвращает ошибку немедленно только при
сбое постановки в очередь, а `onDelivery` вызывается позже, когда придёт
отчёт. `KafkaPublisher` реализует это через `Produce(msg, nil)` с
корреляцией по полю `Opaque`: все события claimed-пачки отправляются сразу
(пайплайн), а единственная фоновая горутина разбирает отчёты о доставке из
общего канала `Events()` по мере поступления и вызывает нужный `onDelivery`.
Реконсайл-тик ждёт (`sync.WaitGroup`) отчётов по всей пачке перед
dead-letter-переносом и следующим тиком.

Порядок публикации в пределах `aggregate_id` при этом не нарушается:
`enable.idempotence=true` вместе с `max.in.flight.requests.per.connection=5`
гарантирует, что брокер подтверждает сообщения одной партиции строго в том
порядке, в котором они были отправлены, даже когда несколько запросов летят
одновременно. Это даёт до ~5-кратного ускорения по сравнению с
последовательным "отправить-и-дождаться" на партицию/соединение, не жертвуя
гарантией порядка.

## Схема базы данных

Схема — версионированные SQL-миграции в `github.com/overtonx/outbox/v4/migrations`,
применяются автоматически при создании `Dispatcher` (идемпотентно, отслеживаются
в таблице `outbox_schema_migrations`). Текущая схема (`migrations/0001_init.sql`):

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_id        CHAR(36)      NOT NULL,
    event_type      VARCHAR(255)  NOT NULL,
    aggregate_type  VARCHAR(255)  NOT NULL,
    aggregate_id    VARCHAR(255)  NOT NULL,
    topic           VARCHAR(255)  NOT NULL,
    content_type    VARCHAR(100)  NOT NULL DEFAULT 'application/json',
    payload         LONGBLOB      NOT NULL,
    headers         JSON          NULL,
    status          TINYINT UNSIGNED NOT NULL DEFAULT 0, -- 0=new,1=sent,2=retry,3=error,4=processing
    attempt_count   INT UNSIGNED  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP(6)  NULL,
    last_error      TEXT          NULL,
    created_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_outbox_events_event_id (event_id),
    KEY idx_outbox_events_claim (status, created_at, id),
    KEY idx_outbox_events_aggregate (aggregate_type, aggregate_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS outbox_deadletters (
    id              BIGINT UNSIGNED PRIMARY KEY,
    event_id        CHAR(36)      NOT NULL,
    event_type      VARCHAR(255)  NOT NULL,
    aggregate_type  VARCHAR(255)  NOT NULL,
    aggregate_id    VARCHAR(255)  NOT NULL,
    topic           VARCHAR(255)  NOT NULL,
    content_type    VARCHAR(100)  NOT NULL DEFAULT 'application/json',
    payload         LONGBLOB      NOT NULL,
    headers         JSON          NULL,
    attempt_count   INT UNSIGNED  NOT NULL,
    last_error      TEXT          NULL,
    created_at      TIMESTAMP(6)  NOT NULL,
    moved_at        TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_outbox_deadletters_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### Очистка старых событий

В отличие от v3, `Dispatcher` **не удаляет** старые `sent`-события или записи
`outbox_deadletters` — это вынесено за пределы библиотеки. Настройте
периодическую очистку самостоятельно (cron, systemd timer, k8s CronJob):

```sql
DELETE FROM outbox_events
WHERE status = 1 AND created_at < NOW() - INTERVAL 1 DAY
LIMIT 1000;

DELETE FROM outbox_deadletters
WHERE moved_at < NOW() - INTERVAL 7 DAY
LIMIT 1000;
```

## Сериализация

`Outbox` использует интерфейс `serializer.Serializer` для преобразования payload
перед записью в БД. Тип сериализации сохраняется в колонке `content_type` и
передаётся потребителям Kafka через заголовок `content-type`.

### Доступные сериализаторы

Пакет: `github.com/overtonx/outbox/v4/serializer`

| Тип                | `content_type`         | Описание                        |
|--------------------|------------------------|---------------------------------|
| `JSONSerializer`   | `application/json`     | JSON-кодирование (по умолчанию) |
| `ProtoSerializer`  | `application/protobuf` | Protobuf binary encoding        |

### JSONSerializer

```go
import "github.com/overtonx/outbox/v4/serializer"

ob := outbox.New(db, serializer.JSONSerializer{})
```

### ProtoSerializer

Payload должен реализовывать `proto.Message`.

```go
import "github.com/overtonx/outbox/v4/serializer"

ob := outbox.New(db, serializer.ProtoSerializer{})
store := ob.EventStore()
err := store.SaveWithDB(ctx, tx, outbox.Event{
    EventType:     "order.created",
    AggregateType: "order",
    AggregateID:   "order-1",
    Topic:         "orders",
    Payload:       myProtoMessage, // proto.Message
})
```

### Свой сериализатор

```go
import "github.com/overtonx/outbox/v4/serializer"

type AvroSerializer struct{ schema string }

func (s AvroSerializer) Marshal(v interface{}) ([]byte, error) {
    // avro-кодирование
}

func (s AvroSerializer) ContentType() string { return "application/avro" }

ob := outbox.New(db, AvroSerializer{schema: "..."})
```

## Маппер событий (`EventMapper`)

`EventMapper` — опциональный хук для преобразования `Event` непосредственно
перед сериализацией и записью в БД. Вызывается **после** инъекции трассировочного
контекста и **до** сериализации payload.

```go
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

`WithMapper` возвращает тот же `*EventStore`, поэтому вызовы можно цепочкой.
Если маппер не задан, поведение не изменяется.

## Конфигурация `Dispatcher`

```go
dispatcher, err := ob.Dispatcher(
    outbox.WithPollInterval(5 * time.Second),
    outbox.WithMaxAttempts(5),
    outbox.WithPublisher(myCustomPublisher),
)
if err != nil {
    // ...
}

// Start блокируется до отмены ctx или Stop().
go dispatcher.Start(context.Background())
```

### Опции

-   `WithPollInterval(time.Duration)`: интервал reconcile-цикла лидера и
    интервал, с которым standby-инстансы пытаются переизбраться. (2 секунды)
-   `WithBatchSize(int)`: количество событий за один тик, 1–10000. (100)
-   `WithMaxAttempts(int)`: максимум попыток публикации перед dead-letter. (3)
-   `WithProcessingLeaseTimeout(time.Duration)`: сколько событие может провисеть
    в статусе "в обработке" (например, после падения лидера), прежде чем будет
    переклеймлено в том же reconcile-запросе. (60 секунд)
-   `WithLockName(string)`: имя advisory-лока для выбора лидера. По умолчанию
    выводится из имени схемы БД.
-   `WithBackoffStrategy(BackoffStrategy)`: стратегия задержки между ретраями.
-   `WithPublisher(Publisher)`: собственная реализация `Publisher`.
-   `WithKafkaConfig(KafkaConfig)`: создаёт `KafkaPublisher` с переданной
    конфигурацией.
-   `WithLogger(*zap.Logger)`: логирование.
-   `WithMetrics(MetricsCollector)`: коллектор метрик (по умолчанию — OTel).

### Проверка лидерства

```go
if dispatcher.IsLeader() {
    // текущий процесс сейчас активно публикует события
}
```

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

`DefaultKafkaConfig()` настроен для сохранения порядка и надёжной доставки:

```go
kafka.ConfigMap{
    "enable.idempotence":                    true,
    "acks":                                  "all",
    "max.in.flight.requests.per.connection": 5, // максимум при enable.idempotence=true
    "retries":                                2147483647, // ограничено delivery.timeout.ms, не числом попыток
    "retry.backoff.ms":                       100,
    "delivery.timeout.ms":                    120000,
    "linger.ms":                               10,
    "compression.type":                       "snappy",
}
```

`max.in.flight.requests.per.connection=5` — не рекомендация, а требование
librdkafka при `enable.idempotence=true` (значения больше 5 недопустимы).
Ретраи не ограничиваются малым числом попыток — это противоречило бы
идемпотентности; общее время на доставку одного сообщения ограничивается
`delivery.timeout.ms`.

### Опции `KafkaConfig`

-   `Topic`: топик по умолчанию (используется, если топик не задан в событии).
-   `ProducerProps`: параметры нативного Kafka-продюсера (`confluent-kafka-go`).
-   `HeaderBuilder`: функция для создания заголовков Kafka-сообщения.

### Заголовки Kafka

Каждое сообщение автоматически получает заголовки: `event_id`, `event_type`,
`aggregate_type`, `aggregate_id`, `content-type`. Дополнительные заголовки
передаются через `Event.Headers`, включая трассировочный контекст (`traceparent`
и т.п.), пробрасываемый автоматически при сохранении события. Системные
заголовки защищены от переопределения пользовательскими данными.

## Публикация сообщений и выбор топика

1.  **Приоритет у события**: если `Topic` задан в событии, сообщение идёт в него.
2.  **Топик по умолчанию**: если `Topic` пустой, используется `KafkaConfig.Topic`.

## Трассировка

При сохранении события (`EventStore.Save`/`SaveWithDB`) активный OTel-контекст
из `ctx` пробрасывается в `Event.Headers` (`traceparent` и т.п.) через
глобальный `TextMapPropagator`, а при публикации — переносится в заголовки
Kafka-сообщения без изменений. Потребитель на другом конце Kafka может
извлечь эти заголовки и создать спан, **связанный** с сохранённым контекстом
(а не дочерний — между записью в БД и доставкой в Kafka проходит время,
обычная родитель/потомок-семантика здесь не применима).

## Миграция с v3 на v4

### Установка

```bash
go get github.com/overtonx/outbox/v4
```

```go
// было
import "github.com/overtonx/outbox/v3"

// стало
import (
    "github.com/overtonx/outbox/v4"
    "github.com/overtonx/outbox/v4/serializer"
)
```

### Убрано

-   Отдельные воркеры и опции `CleanupService`/`WithCleanupInterval`,
    `WithSentEventsRetention`, `WithDeadLetterRetention` — очистка старых
    записей больше не встроена, см. раздел "Очистка старых событий" выше.
-   Отдельный воркер `StuckEventService`/`WithStuckEventTimeout`/
    `WithStuckEventCheckInterval` — восстановление зависших событий теперь
    часть обычного claim-запроса, см. `WithProcessingLeaseTimeout`.
-   `PrometheusMetricsCollector` — был пустой заглушкой без интеграции с
    Prometheus. Используйте `MetricsCollector` (OTel) — вывод в Prometheus
    настраивается на уровне `MeterProvider` хост-приложения.
-   Deprecated-функция `outbox.SaveEvent` — используйте `EventStore.Save`/`SaveWithDB`.
-   `Dispatcher.GetMetrics()` — используйте `WithMetrics`/`WithLogger` для
    наблюдаемости и `IsLeader()` для проверки текущего статуса лидера.

### Изменено

-   `Dispatcher.Start(ctx)` теперь возвращает `error` и блокируется до отмены
    `ctx` или вызова `Stop()` (было: ничего не возвращал, обычно вызывался
    через `go dispatcher.Start(ctx)` — так и осталось, просто проверяйте
    ошибку).
-   `Dispatcher.IsStarted()` заменён на `Dispatcher.IsLeader()` — при нескольких
    инстансах "запущен" и "активно публикует" не одно и то же.
-   Схема БД теперь версионированные `.sql`-миграции
    (`github.com/overtonx/outbox/v4/migrations`), а не `CREATE TABLE IF NOT
    EXISTS` в Go-строках — структура таблиц не изменилась.
-   `DefaultKafkaConfig()` теперь явно выставляет
    `max.in.flight.requests.per.connection=5` и не ограничивает `retries`
    маленьким числом — см. раздел про Kafka-конфигурацию выше.
-   `Publisher.Publish` теперь асинхронный:
    `Publish(ctx, event, onDelivery func(error)) error` вместо
    `Publish(ctx, event) error`. Возвращаемая ошибка означает только сбой
    постановки в очередь; результат самой доставки приходит позже через
    `onDelivery`. Если у вас есть собственная реализация `Publisher`,
    обновите её сигнатуру — см. "Пайплайн публикации и асинхронные отчёты о
    доставке".

### Новое

-   Безопасный запуск нескольких инстансов `Dispatcher` против одной БД —
    см. "Гарантия порядка при нескольких инстансах".
-   `WithLockName`, `WithProcessingLeaseTimeout`.
-   Пайплайнированная асинхронная публикация: события одной claimed-пачки
    отправляются в Kafka без ожидания подтверждения каждого по отдельности —
    см. "Пайплайн публикации и асинхронные отчёты о доставке".
