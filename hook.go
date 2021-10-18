package otredis

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// hook is an implementation of redis.Hook that reports cmds as spans.
type hook struct {
	tracer opentracing.Tracer
}

type ApplyOption func(h *hook)

func WithTracer(tracer opentracing.Tracer) ApplyOption {
	return func(h *hook) {
		h.tracer = tracer
	}
}

// NewHook returns a redis.Hook that reports cmds as spans.
// for default, hook will use global tracer for tracing.
func NewHook(opts ...ApplyOption) redis.Hook {
	h := &hook{
		tracer: opentracing.GlobalTracer(),
	}
	for _, apply := range opts {
		apply(h)
	}
	return h
}

// BeforeProcess initiates the span for the redis cmd
func (r *hook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	span, ctx := opentracing.StartSpanFromContextWithTracer(ctx, r.tracer, getCmdName(cmd))
	ext.Component.Set(span, "redis")
	ext.DBType.Set(span, "redis")
	ext.SpanKind.Set(span, "client")
	ext.DBStatement.Set(span, fmt.Sprintf("%v", cmd.Args()))
	return ctx, nil
}

// AfterProcess ends the initiated span from BeforeProcess
func (r *hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		if err := cmd.Err(); err != nil {
			handleError(ctx, "redis.error", span, err)
		}
		span.Finish()
	}
	return nil
}

// BeforeProcessPipeline initiates the span for the redis cmds
func (r *hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	// Join all cmd names with ", ".
	var cmdNameBuf bytes.Buffer
	for i, cmd := range cmds {
		if i != 0 {
			cmdNameBuf.WriteString(", ")
		}
		cmdNameBuf.WriteString(getCmdName(cmd))
	}

	pipelineSpan, ctx := opentracing.StartSpanFromContextWithTracer(ctx, r.tracer, cmdNameBuf.String())
	ext.Component.Set(pipelineSpan, "redis")
	ext.DBType.Set(pipelineSpan, "redis")
	ext.SpanKind.Set(pipelineSpan, "client")

	for i := len(cmds) - 1; i >= 0; i-- {
		cmdName := getCmdName(cmds[i])

		span, _ := opentracing.StartSpanFromContextWithTracer(ctx, r.tracer, cmdName)
		ext.Component.Set(span, "redis")
		ext.DBType.Set(span, "redis")
		ext.SpanKind.Set(span, "client")
		ext.DBStatement.Set(span, fmt.Sprintf("%v", cmds[i].Args()))
	}
	return ctx, nil
}

// AfterProcessPipeline ends the initiated span from BeforeProcessPipeline
func (r *hook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.Finish()
	}
	return nil
}

func getCmdName(cmd redis.Cmder) string {
	cmdName := strings.ToUpper(cmd.Name())
	if cmdName == "" {
		cmdName = "(empty command)"
	}
	return cmdName
}

func handleError(ctx context.Context, errorTag string, span opentracing.Span, err error) {
	if err != redis.Nil && err != nil {
		span.SetTag(string(ext.Error), true)
		span.SetTag(errorTag, err.Error())
	}
}
