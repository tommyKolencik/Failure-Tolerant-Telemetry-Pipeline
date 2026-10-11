"""Scaffold: future FastAPI ingest, health, statistics, and query endpoints.

M1/M2: implement POST /v1/events/batch, GET /healthz, and GET /readyz. Enforce
the body byte limit while reading, before JSON parsing. Validate the complete
batch and acknowledge only after the store commits it atomically.

Later: add GET /v1/stats with a documented recent window/row cap and
GET /v1/events with source/kind filters and bounded pagination. Keep blocking
database work out of async request handlers. Use one collector process initially.

No FastAPI application or endpoints are implemented yet.
"""
