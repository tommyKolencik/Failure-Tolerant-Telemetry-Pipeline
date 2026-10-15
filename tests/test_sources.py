"""Synthetic source behavior, plus compatibility with durable local storage."""
from itertools import islice
import json
import random
import subprocess
import sys

from pydantic import ValidationError
import pytest

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from shared.schema import canonical_event_bytes, event_key, validate_event

FIXED_BOOT_ID = "6c2fd9a088364f158ad86285e097da41"


def test_seeded_content_reproduces_with_explicit_session_identity():
    first = SyntheticSource(seed=7, boot_id=FIXED_BOOT_ID)
    second = SyntheticSource(seed=7, boot_id=FIXED_BOOT_ID)
    assert [canonical_event_bytes(item) for item in first.events(1_000)] == [
        canonical_event_bytes(item) for item in second.events(1_000)
    ]


def test_new_sessions_get_new_identity_and_restart_sequence():
    first = SyntheticSource(seed=7).next_event()
    second = SyntheticSource(seed=7).next_event()
    assert first.boot_id != second.boot_id
    assert first.seq == second.seq == 0
    assert first.source_uptime_us == second.source_uptime_us == 0
    assert first.payload == second.payload
    assert event_key(first) != event_key(second)


def test_sequences_and_simulated_uptime_continue_across_calls():
    source = SyntheticSource(uptime_step_us=25_000)
    events = list(source.events(3)) + [source.next_event()] + list(source.events(2))
    assert [item.seq for item in events] == list(range(6))
    assert [item.source_uptime_us for item in events] == [seq * 25_000 for seq in range(6)]
    assert len({event_key(item) for item in events}) == 6
    assert all(15 <= item.payload.temperature_c <= 30 for item in events)
    assert all(item.payload.sensor_ok and item.payload.error_code is None for item in events)


def test_unconsumed_generator_does_not_advance_source_or_preallocate_events():
    source = SyntheticSource()
    pending = source.events(1_000_000_000)
    assert source.next_event().seq == 0
    assert [item.seq for item in islice(pending, 3)] == [1, 2, 3]
    assert list(source.events(0)) == []
    assert source.next_event().seq == 4


def test_randomness_in_other_code_does_not_change_source_sequence():
    source = SyntheticSource(seed=7)
    expected = SyntheticSource(seed=7)
    first = source.next_event()
    assert first.payload == expected.next_event().payload
    state = random.getstate()
    try:
        random.seed(999)
        for _ in range(100):
            random.random()
        assert source.next_event().payload == expected.next_event().payload
    finally:
        random.setstate(state)
    assert [item.payload for item in SyntheticSource(seed=8).events(10)] != [
        item.payload for item in SyntheticSource(seed=7).events(10)
    ]


@pytest.mark.parametrize("settings", [
    {"seed": True}, {"seed": "7"}, {"seed": 7.0},
    {"uptime_step_us": 0}, {"uptime_step_us": True}, {"uptime_step_us": 2**63},
])
def test_invalid_generator_settings_fail_at_construction(settings):
    with pytest.raises(ValueError):
        SyntheticSource(**settings)


@pytest.mark.parametrize("settings", [
    {"source_id": "bad name"}, {"source_id": "name\n"}, {"source_id": "a" * 65},
    {"boot_id": ""}, {"boot_id": "A" * 32},
])
def test_shared_schema_validates_session_identity(settings):
    with pytest.raises(ValidationError):
        SyntheticSource(**settings)


@pytest.mark.parametrize("count", [-1, True, "5", 5.0, 2**63])
def test_invalid_count_fails_before_generator_consumption(count):
    with pytest.raises(ValueError):
        SyntheticSource().events(count)


def test_uptime_exhaustion_does_not_wrap_or_reset_identity():
    source = SyntheticSource(uptime_step_us=2**63 - 1)
    first, second = list(source.events(2))
    assert first.source_uptime_us == 0
    assert second.source_uptime_us == 2**63 - 1
    assert first.boot_id == second.boot_id
    for _ in range(2):
        with pytest.raises(OverflowError):
            source.next_event()


def test_generated_events_survive_outbox_restart_without_identity_changes(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    source = SyntheticSource(seed=7)
    generated = list(source.events(100))
    for item in generated:
        assert outbox.publish(item).accepted
    reopened = Outbox(outbox.path)
    recovered = reopened.peek_batch()
    assert [canonical_event_bytes(item) for item in recovered] == [
        canonical_event_bytes(item) for item in generated
    ]
    next_session = SyntheticSource(seed=7).next_event()
    assert next_session.boot_id != generated[0].boot_id
    assert next_session.seq == generated[0].seq == 0
    assert reopened.publish(next_session).status == "inserted"


def test_preview_command_emits_valid_ndjson():
    result = subprocess.run(
        [sys.executable, "-m", "agent.sources", "--count", "3", "--seed", "7"],
        capture_output=True, text=True, check=True,
    )
    events = [validate_event(json.loads(line)) for line in result.stdout.splitlines()]
    assert [item.seq for item in events] == [0, 1, 2]
    assert len({item.boot_id for item in events}) == 1
