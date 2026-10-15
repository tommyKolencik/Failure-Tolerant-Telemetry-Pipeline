# Host and request telemetry

Read these files in order:

1. [shared/schema.py](../shared/schema.py): strict fields and cross-field
   validators define three event kinds. `Field(discriminator="kind")` selects
   the payload model; `TypeAdapter` validates the union. Existing models are
   dumped and revalidated to catch mutations.
2. [agent/sources.py](../agent/sources.py): fresh host sessions own increasing
   sequences and monotonic uptime. A lock protects concurrent request IDs.
   CPU sampling stays on one thread; warmup and intervals below 100 ms produce
   null CPU fields. RAM is total/available physical memory in bytes.
3. [demo/app.py](../demo/app.py): ASGI middleware measures through final-body
   send, including streaming. It excludes later publishing/background work
   and client TCP receipt. Labels use route templates, with `/__unmatched__`
   for unknown paths. Recording failures increment counters and preserve responses.
4. [agent/main.py](../agent/main.py) and [collector/store.py](../collector/store.py):
   the host uploader also drains the demo's shared outbox. Collector request
   percentiles use its capped recent retained sample, not delivery latency.

Pydantic references:
[configuration](https://docs.pydantic.dev/latest/concepts/config/),
[validators](https://docs.pydantic.dev/latest/concepts/validators/),
[discriminated unions](https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions),
[TypeAdapter](https://docs.pydantic.dev/latest/concepts/type_adapter/),
[serialization](https://docs.pydantic.dev/latest/concepts/serialization/).

Sampling/timing:
[psutil CPU](https://psutil.io/api/#psutil.cpu_percent),
[physical memory](https://psutil.io/api/#psutil.virtual_memory),
[ASGI middleware](https://starlette.dev/middleware/#pure-asgi-middleware).

Run commands: [README](../README.txt). Coverage: [test_metrics.py](../tests/test_metrics.py).
Historical native evidence: [metrics-verification-mac.json](metrics-verification-mac.json).
