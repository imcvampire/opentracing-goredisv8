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
type hook struct{}

// NewHook returns a redis.Hook that reports cmds as spans.
func NewHook() redis.Hook {
	return &hook{}
}

// BeforeProcess initiates the span for the redis cmd
func (r *hook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, getCmdName(cmd))
	ext.Component.Set(span, "redis")
	ext.DBType.Set(span, "redis")
	ext.SpanKind.Set(span, "client")
	ext.DBStatement.Set(span, fmt.Sprintf("%v", cmd.Args()))
	return ctx, nil
}

// AfterProcess ends the initiated span from BeforeProcess
func (r *hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
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

	pipelineSpan, ctx := opentracing.StartSpanFromContext(ctx, "(pipeline)")
	ext.Component.Set(pipelineSpan, "redis")
	ext.DBType.Set(pipelineSpan, "redis")
	ext.SpanKind.Set(pipelineSpan, "client")

	for i := len(cmds); i > 0; i-- {
		cmdName := strings.ToUpper(cmds[i-1].Name())
		if cmdName == "" {
			cmdName = "(empty command)"
		}

		span, _ := opentracing.StartSpanFromContext(ctx, cmdName)
		ext.Component.Set(span, "redis")
		ext.DBType.Set(span, "redis")
		ext.SpanKind.Set(span, "client")
		ext.DBStatement.Set(span, fmt.Sprintf("%v", cmds[i-1].Args()))
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
