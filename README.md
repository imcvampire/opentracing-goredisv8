# opentracing-goredisv8
A opentracing module for go-redis v8 and later 

## How to integrate

```go
    // Init tracer ...
    rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
    h := hook.NewHook(hook.WithTracer(tracer))
    rdb.AddHook(h)
```
please check detail at examples.
