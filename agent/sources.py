"""Scaffold: synthetic, host-metric, and ESP32 serial event sources.

M1: generate deterministic payloads with a fixed seed, an increasing sequence,
and a fresh boot ID per restart. M3: collect CPU/RAM through psutil. M4: receive
pyserial NDJSON, preserve firmware identities, handle partial lines/reconnects,
and discard oversized frames through their next newline. Send every accepted
source event through the shared outbox publish boundary.

No producer or serial reader is implemented yet.
"""
