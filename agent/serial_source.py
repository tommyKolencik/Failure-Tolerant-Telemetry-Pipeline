"""Bounded serial NDJSON ingestion that preserves firmware identity."""
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass
import json
import math
from threading import Event as StopEvent
import time
from typing import Protocol

from pydantic import ValidationError
import serial

from shared.config import SERIAL_LINE_MAX_BYTES
from shared.schema import Event, validate_event

READ_CHUNK_BYTES = 256  # Provisional host read bound, independent of line length.


@dataclass
class FrameStats:
    complete_lines: int = 0
    oversized_lines: int = 0
    oversized_discarded_bytes: int = 0
    resync_lines: int = 0
    resync_discarded_bytes: int = 0
    truncated_frames: int = 0
    partial_discarded_bytes: int = 0


class NDJSONFramer:
    """Keep at most limit-1 bytes; an overlong frame discards through its LF."""

    def __init__(self, limit: int = SERIAL_LINE_MAX_BYTES) -> None:
        if type(limit) is not int or not 2 <= limit <= SERIAL_LINE_MAX_BYTES:
            raise ValueError("line limit must be an integer in 2..1024, including LF")
        self.limit = limit
        self.stats = FrameStats()
        self._buffer = bytearray()
        self._discard: str | None = None

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def clear_partial(self) -> None:
        if self._buffer:
            self.stats.truncated_frames += 1
            self.stats.partial_discarded_bytes += len(self._buffer)
            self._buffer.clear()
        self._discard = None

    def synchronize(self) -> None:
        self.clear_partial()
        # Discard through the first LF on every open, including startup. This may
        # drop one valid unaccepted event; fragments from different ports never join.
        self._discard = "resync"

    def push(self, byte: int) -> bytes | None:
        if byte == 10:
            self.stats.complete_lines += 1
        if self._discard is not None:
            if self._discard == "oversized":
                self.stats.oversized_discarded_bytes += 1
            else:
                self.stats.resync_discarded_bytes += 1
                self.stats.resync_lines += int(byte == 10)
            if byte == 10:
                self._discard = None
            return None
        if byte == 10:
            frame = bytes(self._buffer)
            self._buffer.clear()
            return frame
        if len(self._buffer) == self.limit - 1:
            # Count overflow once and discard the entire remaining line.
            self.stats.oversized_lines += 1
            self.stats.oversized_discarded_bytes += len(self._buffer) + 1
            self._buffer.clear()
            self._discard = "oversized"
            return None
        self._buffer.append(byte)
        return None

    def feed(self, chunk: bytes) -> Iterator[bytes]:
        if not isinstance(chunk, bytes) or len(chunk) > READ_CHUNK_BYTES:
            raise ValueError("feed requires at most 256 bytes")
        for byte in chunk:
            frame = self.push(byte)
            if frame is not None:
                yield frame


def _reject_constant(value: str) -> None:
    raise ValueError("nonfinite JSON constants are unsupported")


def _unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object member")
        result[key] = value
    return result


def decode_frame(frame: bytes) -> Event:
    if not isinstance(frame, bytes) or len(frame) >= SERIAL_LINE_MAX_BYTES:
        raise ValueError("frame must contain at most 1023 payload bytes before LF")
    # Reject malformed JSON before validating its schema.
    raw = json.loads(frame.decode("utf-8"), parse_constant=_reject_constant,
                     object_pairs_hook=_unique_object)
    return validate_event(raw)


class SerialPort(Protocol):
    @property
    def in_waiting(self) -> int: ...
    def read(self, size: int) -> bytes: ...
    def close(self) -> None: ...


def open_port(port: str, baudrate: int, timeout: float) -> SerialPort:
    # Set DTR/RTS before opening. Drivers may still pulse them; verify on hardware.
    connection = serial.Serial(port=None, baudrate=baudrate, timeout=timeout,
                               write_timeout=timeout, exclusive=True)
    try:
        connection.dtr = False
        connection.rts = False
        connection.port = port
        connection.open()
        return connection
    except BaseException:
        connection.close()
        raise


class SerialSource:
    """One reader owned by the controller thread; poll_event may return None."""

    def __init__(self, port: str, *, baudrate: int = 115_200, timeout: float = 0.1,
                 reconnect_delay: float = 1.0,
                 port_factory: Callable[[str, int, float], SerialPort] = open_port,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if not isinstance(port, str) or not port.strip() or len(port) > 512 or any(ord(c) < 32 for c in port):
            raise ValueError("serial port must be a nonblank path of at most 512 characters")
        if type(baudrate) is not int or not 1 <= baudrate <= 4_000_000:
            raise ValueError("baudrate must be an integer in 1..4000000")
        for name, value, low, high in (("timeout", timeout, 0.01, 0.5), ("reconnect_delay", reconnect_delay, 0.05, 60)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not low <= value <= high or not math.isfinite(value):
                raise ValueError(f"{name} must be finite and in {low}..{high}")
        self.port, self.baudrate = port, baudrate
        self.timeout, self.reconnect_delay = timeout, reconnect_delay
        self._factory, self._clock = port_factory, clock
        self.framer = NDJSONFramer()
        self._connection: SerialPort | None = None
        self._chunk, self._position = b"", 0
        self._retry_at = 0.0
        self._closed = False
        # Latest validated firmware session.
        self.source_id: str | None = None
        self.boot_id: str | None = None
        self._counts = {name: 0 for name in ("connections", "open_errors", "read_errors", "close_errors",
                         "bytes_read", "valid_events", "empty_lines", "invalid_json", "invalid_schema",
                         "unprocessed_read_bytes")}
        self._last_transport_error: str | None = None

    def snapshot(self) -> dict:
        return {**self._counts, **asdict(self.framer.stats), "connected": self._connection is not None,
                "buffered_frame_bytes": self.framer.buffered_bytes,
                "buffered_read_bytes": len(self._chunk) - self._position,
                "last_transport_error": self._last_transport_error}

    def _drop_connection(self) -> None:
        connection, self._connection = self._connection, None
        try:
            if connection is not None:
                connection.close()
        except (OSError, serial.SerialException):
            self._counts["close_errors"] += 1
        finally:
            self._counts["unprocessed_read_bytes"] += len(self._chunk) - self._position
            self._chunk, self._position = b"", 0
            self.framer.clear_partial()

    def close(self) -> None:
        self._drop_connection()
        self._closed = True

    def poll_event(self, stop_event: StopEvent) -> Event | None:
        if self._closed:
            raise RuntimeError("serial source is closed; create a fresh reader")
        if stop_event.is_set():
            return None
        if self._connection is None:
            remaining = self._retry_at - self._clock()
            if remaining > 0:
                stop_event.wait(min(0.1, remaining))
                return None
            try:
                self._connection = self._factory(self.port, self.baudrate, self.timeout)
            except (OSError, serial.SerialException) as exc:
                self._counts["open_errors"] += 1
                self._last_transport_error = str(exc)[:160]
                self._retry_at = self._clock() + self.reconnect_delay
                return None
            self._counts["connections"] += 1
            self._last_transport_error = None
            self.framer.synchronize()
        if stop_event.is_set():
            return None
        if self._position == len(self._chunk):
            try:
                # Read available bytes, or wait for one byte up to timeout. OS/USB
                # buffers are outside this bound.
                size = min(READ_CHUNK_BYTES, max(1, self._connection.in_waiting))
                chunk = self._connection.read(size)
            except (OSError, serial.SerialException) as exc:
                self._counts["read_errors"] += 1
                self._last_transport_error = str(exc)[:160]
                self._drop_connection()
                self._retry_at = self._clock() + self.reconnect_delay
                return None
            if not isinstance(chunk, bytes) or len(chunk) > size:
                raise RuntimeError("serial driver violated the bounded read contract")
            self._counts["bytes_read"] += len(chunk)
            self._chunk, self._position = chunk, 0
        while self._position < len(self._chunk) and not stop_event.is_set():
            byte = self._chunk[self._position]
            self._position += 1
            frame = self.framer.push(byte)
            if frame is None:
                continue
            if not frame.strip():
                self._counts["empty_lines"] += 1
                continue
            try:
                event = decode_frame(frame)
            except ValidationError:
                self._counts["invalid_schema"] += 1
                continue
            except (ValueError, UnicodeDecodeError, RecursionError):
                self._counts["invalid_json"] += 1
                continue
            self._counts["valid_events"] += 1
            self.source_id, self.boot_id = event.source_id, event.boot_id
            return event
        return None
