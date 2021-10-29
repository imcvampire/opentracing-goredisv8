package main

import (
	"context"
	"fmt"
	"io"

	"github.com/go-redis/redis/v8"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

	hook "github.com/imcvampire/opentracing-goredisv8"
)

const (
	serviceName = "RedisHookExample"
)

func main() {
	tracer, closer := initTracer(serviceName)
	defer closer.Close()
	span := tracer.StartSpan("Handle request")
	span.Finish()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	h := hook.NewHook(hook.WithTracer(tracer))
	rdb.AddHook(h)

	ctx := context.Background()

	ctx = opentracing.ContextWithSpan(ctx, span)
	defer span.Finish()
	if err := handleRequest(ctx, rdb); err != nil {
		recordError(ctx, "request.error", span, err)
	}
}

func handleRequest(ctx context.Context, rdb *redis.Client) error {
	if err := rdb.Set(ctx, "First value", "value_1", 0).Err(); err != nil {
		return err
	}
	if err := rdb.Set(ctx, "Second value", "value_2", 0).Err(); err != nil {
		return err
	}

	if err := rdb.Del(ctx, "First value").Err(); err != nil {
		return err
	}
	if err := rdb.Del(ctx, "Second value").Err(); err != nil {
		return err
	}

	return nil
}

// initTracer returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func initTracer(serviceName string) (opentracing.Tracer, io.Closer) {
	cfg := &config.Configuration{
		Sampler: &config.SamplerConfig{},
	}
	cfg.ServiceName = serviceName
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	_, err := config.FromEnv()
	println(cfg.Sampler.SamplingServerURL, cfg.Sampler.Param)
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot load config from env: %v\n", err))
	}
	cfg.Reporter = &config.ReporterConfig{
		LogSpans: true,
	}
	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return tracer, closer
}

func recordError(ctx context.Context, errorTag string, span opentracing.Span, err error) {
	if err != redis.Nil && err != nil {
		span.SetTag(string(ext.Error), true)
		span.SetTag(errorTag, err.Error())
	}
}
