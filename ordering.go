package outbox

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// runOrderedLeader — цикл лидера для Publisher, реализующего OrderedPublisher
// (см. publisher.go; сейчас — только KafkaPublisher). Вместо построчных
// статусов new/retry/error используется единый глобальный чекпоинт
// (outbox_checkpoints.last_confirmed_id) и watermark, продвигаемый строго по
// непрерывному префиксу подтверждений (checkpointTracker). Любая ошибка
// доставки — фатальная ошибка producer'а или ошибка конкретного сообщения —
// останавливает claim новых событий, пересоздаёт producer и возобновляет
// поток строго с последнего персистентного чекпоинта: точечных ретраев
// отдельных сообщений здесь нет, чтобы не переупорядочить партицию, когда
// более позднее (по тому же ключу) сообщение уже успешно доставлено.
func (d *Dispatcher) runOrderedLeader(ctx context.Context) {
	watermark, err := d.checkpoints.Load(ctx, d.lockName)
	if err != nil {
		d.logger.Error("outbox: failed to load checkpoint, aborting leadership", zap.Error(err))
		return
	}
	d.ordered.SeedWatermark(watermark)
	highestClaimed := watermark

	fault := make(chan error, 1)
	reportFault := func(err error) {
		select {
		case fault <- err:
		default:
		}
	}
	d.ordered.SetFatalHandler(reportFault)
	defer d.ordered.SetFatalHandler(nil)

	pollTicker := time.NewTicker(d.pollInterval)
	defer pollTicker.Stop()
	flushTicker := time.NewTicker(d.checkpointFlushInterval)
	defer flushTicker.Stop()

	// poison реализует единственное узаконенное исключение из "никаких
	// точечных ретраев": если одно и то же событие сразу после чекпоинта
	// подряд ломает поток через несколько циклов replay, оно уходит в
	// deadletters, а чекпоинт принудительно продвигается мимо него — иначе
	// poison-сообщение остановило бы весь упорядоченный поток навсегда.
	poison := &poisonStreakTracker{atID: -1}

	claimMore := func() {
		newHighest, err := d.claimAndPublishOrdered(ctx, highestClaimed, reportFault)
		if err != nil {
			reportFault(err)
			return
		}
		highestClaimed = newHighest
	}

	claimMore()

	for {
		select {
		case <-ctx.Done():
			d.flushCheckpoint(context.Background())
			return

		case <-pollTicker.C:
			claimMore()

		case <-flushTicker.C:
			d.flushCheckpoint(ctx)

		case err := <-fault:
			d.logger.Error("outbox: ordered publisher fault, stopping stream and replaying from checkpoint",
				zap.Error(err))
			d.metrics.IncrementCounter("outbox.ordered.fault", nil)

			nextID := d.ordered.Watermark() + 1
			streak := poison.Record(nextID)

			if streak >= d.poisonMessageThreshold {
				if qerr := d.quarantineAndAdvance(ctx, nextID, err); qerr != nil {
					d.logger.Error("outbox: failed to quarantine poison message",
						zap.Int64("id", nextID), zap.Error(qerr))
				} else {
					poison.Reset()
				}
			}

			if rerr := d.ordered.Recreate(); rerr != nil {
				d.logger.Error("outbox: failed to recreate kafka producer, will retry on next tick",
					zap.Error(rerr))
				continue
			}

			persisted, lerr := d.checkpoints.Load(ctx, d.lockName)
			if lerr != nil {
				d.logger.Error("outbox: failed to reload checkpoint after fault", zap.Error(lerr))
				continue
			}
			d.ordered.SeedWatermark(persisted)
			highestClaimed = persisted
			claimMore()
		}
	}
}

// claimAndPublishOrdered забирает из БД события строго после highestClaimed,
// ограниченные окном d.inFlightWindow впереди последнего подтверждённого
// watermark (чтобы объём переотправки после Recreate/replay и рост
// checkpointTracker были ограничены), и публикует их асинхронно — не
// дожидаясь ack'ов, в отличие от построчного пути (publish.go).
func (d *Dispatcher) claimAndPublishOrdered(ctx context.Context, highestClaimed int64, reportFault func(error)) (int64, error) {
	watermark := d.ordered.Watermark()
	window := int64(d.inFlightWindow) - (highestClaimed - watermark)
	if window <= 0 {
		return highestClaimed, nil
	}

	limit := d.batchSize
	if window < int64(limit) {
		limit = int(window)
	}

	events, err := d.claimByWatermark(ctx, highestClaimed, limit)
	if err != nil {
		return highestClaimed, fmt.Errorf("failed to claim events: %w", err)
	}

	for _, event := range events {
		event := event
		if err := d.ordered.Publish(ctx, event, func(deliveryErr error) {
			if deliveryErr != nil {
				reportFault(fmt.Errorf("delivery failed for event id=%d: %w", event.ID, deliveryErr))
			}
		}); err != nil {
			reportFault(fmt.Errorf("failed to enqueue event id=%d: %w", event.ID, err))
			return highestClaimed, nil
		}
		highestClaimed = event.ID
	}

	return highestClaimed, nil
}

// flushCheckpoint персистит текущий watermark в outbox_checkpoints и
// батчем обновляет status уже подтверждённых событий — периодически (по
// d.checkpointFlushInterval), а не на каждый ack.
func (d *Dispatcher) flushCheckpoint(ctx context.Context) {
	watermark := d.ordered.Watermark()

	if err := d.checkpoints.Save(ctx, d.lockName, watermark); err != nil {
		d.logger.Error("outbox: failed to persist checkpoint", zap.Int64("watermark", watermark), zap.Error(err))
		return
	}

	if _, err := d.db.ExecContext(ctx,
		"UPDATE outbox_events SET status = ? WHERE id <= ? AND status <> ?",
		EventRecordStatusSent, watermark, EventRecordStatusSent,
	); err != nil {
		d.logger.Error("outbox: failed to batch-update status for confirmed events", zap.Error(err))
	}
}

// quarantineAndAdvance переносит одно poison-событие в outbox_deadletters и
// принудительно продвигает персистентный чекпоинт мимо него.
func (d *Dispatcher) quarantineAndAdvance(ctx context.Context, id int64, cause error) error {
	d.logger.Error("outbox: poison message detected, quarantining and advancing checkpoint past it",
		zap.Int64("id", id), zap.Error(cause))
	d.metrics.IncrementCounter("outbox.ordered.poison_message", nil)

	if err := d.moveSingleToDeadLetter(ctx, id, cause); err != nil {
		return err
	}
	if err := d.checkpoints.Save(ctx, d.lockName, id); err != nil {
		return fmt.Errorf("failed to force-advance checkpoint past quarantined id %d: %w", id, err)
	}
	return nil
}

// poisonStreakTracker считает число подряд идущих сбоев на одном и том же
// id (первое событие после чекпоинта, на котором заново упирается каждый
// цикл replay). Смена id сбрасывает счётчик — сбой на разных id подряд не
// считается poison-сообщением.
type poisonStreakTracker struct {
	atID   int64
	streak int
}

// Record регистрирует очередной сбой на id и возвращает текущую длину
// серии подряд идущих сбоев именно на этом id.
func (p *poisonStreakTracker) Record(id int64) int {
	if id == p.atID {
		p.streak++
	} else {
		p.atID = id
		p.streak = 1
	}
	return p.streak
}

// Reset обнуляет серию — вызывается после успешного quarantine или когда
// поток снова продвинулся дальше проблемного id.
func (p *poisonStreakTracker) Reset() {
	p.atID = -1
	p.streak = 0
}
