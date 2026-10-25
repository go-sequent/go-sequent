// Package obs provides OpenTelemetry helpers for command latency, conflicts, and outbox depth.
package obs

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/go-sequent/go-sequent"

// Instruments holds commonly used meters/tracers.
type Instruments struct {
	Tracer         trace.Tracer
	CommandLatency metric.Float64Histogram
	CommandsTotal  metric.Int64Counter
	ConflictsTotal metric.Int64Counter
	EventsAppended metric.Int64Counter
	OutboxDepth    metric.Int64ObservableGauge
	outboxDepthFn  func(context.Context) int64
}

// Config for New.
type Config struct {
	// OutboxDepth optionally supplies current outbox depth for the observable gauge.
	OutboxDepth func(context.Context) int64
}

// New builds Instruments using the global MeterProvider / TracerProvider (noop if unset).
func New(cfg Config) (*Instruments, error) {
	tr := otel.Tracer(instrumentationName)
	meter := otel.Meter(instrumentationName)

	lat, err := meter.Float64Histogram("sequent.command.duration_ms",
		metric.WithDescription("CommandService.Execute latency in milliseconds"))
	if err != nil {
		return nil, err
	}
	cmds, err := meter.Int64Counter("sequent.command.total",
		metric.WithDescription("Commands executed"))
	if err != nil {
		return nil, err
	}
	conflicts, err := meter.Int64Counter("sequent.optimistic_conflict.total",
		metric.WithDescription("Optimistic concurrency conflicts"))
	if err != nil {
		return nil, err
	}
	events, err := meter.Int64Counter("sequent.events.appended",
		metric.WithDescription("Events appended"))
	if err != nil {
		return nil, err
	}

	inst := &Instruments{
		Tracer:         tr,
		CommandLatency: lat,
		CommandsTotal:  cmds,
		ConflictsTotal: conflicts,
		EventsAppended: events,
		outboxDepthFn:  cfg.OutboxDepth,
	}

	if cfg.OutboxDepth != nil {
		g, err := meter.Int64ObservableGauge("sequent.outbox.depth",
			metric.WithDescription("Pending outbox messages"))
		if err != nil {
			return nil, err
		}
		inst.OutboxDepth = g
		_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			o.ObserveInt64(g, cfg.OutboxDepth(ctx))
			return nil
		}, g)
		if err != nil {
			return nil, err
		}
	}
	return inst, nil
}

// StartCommand starts a span for ExecuteCommands.
func (i *Instruments) StartCommand(ctx context.Context, commandType string) (context.Context, trace.Span) {
	if i == nil || i.Tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return i.Tracer.Start(ctx, "sequent.Execute",
		trace.WithAttributes(attribute.String("command.type", commandType)))
}

// RecordCommand records latency and outcome.
func (i *Instruments) RecordCommand(ctx context.Context, span trace.Span, commandType string, started time.Time, err error) {
	if i == nil {
		return
	}
	ms := float64(time.Since(started).Milliseconds())
	attrs := []attribute.KeyValue{attribute.String("command.type", commandType)}
	if i.CommandLatency != nil {
		i.CommandLatency.Record(ctx, ms, metric.WithAttributes(attrs...))
	}
	if i.CommandsTotal != nil {
		status := "ok"
		if err != nil {
			status = "error"
		}
		i.CommandsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("command.type", commandType),
			attribute.String("status", status),
		))
	}
	if span != nil {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// RecordConflict increments the optimistic concurrency counter.
func (i *Instruments) RecordConflict(ctx context.Context) {
	if i == nil || i.ConflictsTotal == nil {
		return
	}
	i.ConflictsTotal.Add(ctx, 1)
}

// RecordEvents increments events appended.
func (i *Instruments) RecordEvents(ctx context.Context, n int64) {
	if i == nil || i.EventsAppended == nil || n == 0 {
		return
	}
	i.EventsAppended.Add(ctx, n)
}
