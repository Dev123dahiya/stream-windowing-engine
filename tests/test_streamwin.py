import random

import pytest

from streamwin import Event, Session, Sliding, Tumbling, WindowedAggregator, batch_reference
from streamwin.engine import final_results


def test_window_assignment():
    assert Tumbling(10).assign(0) == [(0, 10)] and Tumbling(10).assign(9) == [(0, 10)] and Tumbling(10).assign(10) == [(10, 20)]
    assert Sliding(10, 5).assign(12) == [(5, 15), (10, 20)]
    assert Sliding(15, 5).assign(12) == [(0, 15), (5, 20), (10, 25)]
    assert Sliding(10, 10).assign(12) == Tumbling(10).assign(12)
    assert Session(30).assign(100) == [(100, 130)]
    with pytest.raises(ValueError):
        Sliding(5, 10)


def test_tumbling_counts_fire_when_the_watermark_passes():
    agg = WindowedAggregator(Tumbling(10))
    assert agg.process(Event("a", 1)) == []
    assert agg.process(Event("a", 5)) == []
    fired = agg.process(Event("a", 12))             # watermark moves to 12, closing [0, 10)
    assert [(r.key, r.start, r.end, r.value, r.revision) for r in fired] == [("a", 0, 10, 2, 0)]
    assert [(r.start, r.value) for r in agg.flush()] == [(10, 1)]
    assert agg.open_windows == 0


def test_keys_are_aggregated_independently():
    events = [Event("a", 1, 5), Event("b", 2, 7), Event("a", 3, 1), Event("b", 11, 2)]
    assert final_results(WindowedAggregator(Tumbling(10)).run(events)) == {("a", 0, 10): 6, ("b", 0, 10): 7, ("b", 10, 20): 2}


def test_out_of_order_events_within_the_bound_are_counted_once_and_on_time():
    agg = WindowedAggregator(Tumbling(10), max_out_of_order=5)
    out = []
    for ts in [1, 12, 8, 14, 3]:        # 8 and 3 arrive after 12 and 14 but belong to [0, 10)
        out += agg.process(Event("k", ts))
    assert out == [] and agg.watermark == 9            # 14 - 5: the first window is still open
    out += agg.process(Event("k", 15))                  # watermark 10 closes it
    assert [(r.start, r.value, r.revision) for r in out] == [(0, 3, 0)]
    assert agg.late == []


def test_late_data_gets_a_correction_then_is_routed_aside_after_allowed_lateness():
    agg = WindowedAggregator(Tumbling(10), allowed_lateness=20)
    agg.process(Event("k", 5))
    first = agg.process(Event("k", 11))
    assert [(r.value, r.revision) for r in first] == [(1, 0)]
    correction = agg.process(Event("k", 7))             # late, but within the 20 units allowed
    assert [(r.start, r.value, r.revision) for r in correction] == [(0, 2, 1)]
    agg.process(Event("k", 31))                         # watermark 31 > 10 + 20: the window is gone
    assert agg.process(Event("k", 8)) == []
    assert [(l.event.ts, l.watermark) for l in agg.late] == [(8, 31)]
    assert final_results(first + correction)[("k", 0, 10)] == 2


def test_state_is_freed_once_windows_expire():
    agg = WindowedAggregator(Tumbling(10), allowed_lateness=5)
    for ts in range(0, 1000):
        agg.process(Event(ts % 7, ts))
    assert agg.open_windows <= 7 * 3                    # bounded by keys x live windows, not by stream length
    assert len(agg._emitted) <= 7 * 3


def test_sliding_windows_count_each_event_in_every_window_it_falls_in():
    out = final_results(WindowedAggregator(Sliding(10, 5)).run([Event("k", 3), Event("k", 7), Event("k", 12)]))
    assert out == {("k", -5, 5): 1, ("k", 0, 10): 2, ("k", 5, 15): 2, ("k", 10, 20): 1}


def test_sessions_split_on_gaps_and_merge_when_bridged():
    agg = WindowedAggregator(Session(10))
    out = agg.run([Event("u", 0), Event("v", 2), Event("u", 4), Event("u", 30), Event("u", 35)])
    assert final_results(out) == {("u", 0, 14): 2, ("u", 30, 45): 2, ("v", 2, 12): 1}
    bridged = WindowedAggregator(Session(10), max_out_of_order=50).run(
        [Event("u", 0), Event("u", 18), Event("u", 9)])       # 9 arrives last and joins both sessions
    assert final_results(bridged) == {("u", 0, 28): 3}


def test_custom_aggregates():
    stats = WindowedAggregator(Tumbling(10), init=lambda: {"n": 0, "sum": 0, "max": None},
                               add=lambda a, v: {"n": a["n"] + 1, "sum": a["sum"] + v, "max": v if a["max"] is None else max(a["max"], v)})
    (r,) = stats.run([Event("k", 1, 4), Event("k", 2, 10), Event("k", 3, 1)])
    assert r.value == {"n": 3, "sum": 15, "max": 10}
    distinct = WindowedAggregator(Session(5), init=set, add=lambda a, v: a | {v}, merge=lambda a, b: a | b)
    assert final_results(distinct.run([Event("k", 0, "x"), Event("k", 3, "y"), Event("k", 4, "x")])) == {("k", 0, 9): {"x", "y"}}


def make_stream(rng, n=400, keys=5, jitter=20):
    """In-order events, then shuffled so no event is more than ``jitter`` behind the newest one seen."""
    ordered = [Event(rng.randrange(keys), ts, rng.randint(1, 9)) for ts in sorted(rng.sample(range(n * 3), n))]
    arrival = sorted(ordered, key=lambda e: e.ts + rng.uniform(0, jitter))
    return ordered, arrival


@pytest.mark.parametrize("window", [Tumbling(25), Sliding(40, 10), Session(15)], ids=["tumbling", "sliding", "session"])
@pytest.mark.parametrize("seed", range(5))
def test_streaming_equals_batch_when_disorder_is_within_the_bound(window, seed):
    """The core property: with a sufficient out-of-order bound, arrival order does not change the answer."""
    ordered, arrival = make_stream(random.Random(seed))
    assert arrival != ordered
    agg = WindowedAggregator(window, max_out_of_order=20)
    results = agg.run(arrival)
    assert final_results(results) == batch_reference(ordered, window)
    assert agg.late == [] and all(r.revision == 0 for r in results)     # nothing was late, so nothing was corrected
    assert agg.open_windows == 0


@pytest.mark.parametrize("window", [Tumbling(25), Sliding(40, 10)], ids=["tumbling", "sliding"])
def test_too_small_a_bound_still_converges_through_corrections(window):
    ordered, arrival = make_stream(random.Random(3), jitter=60)
    agg = WindowedAggregator(window, max_out_of_order=5, allowed_lateness=10_000)
    results = agg.run(arrival)
    assert any(r.revision > 0 for r in results)                         # early answers were wrong...
    assert final_results(results) == batch_reference(ordered, window)   # ...and were fixed
    assert agg.late == []


def test_dropped_plus_counted_accounts_for_every_event():
    ordered, arrival = make_stream(random.Random(8), jitter=80)
    agg = WindowedAggregator(Tumbling(25), max_out_of_order=5, allowed_lateness=0)
    counted = sum(final_results(agg.run(arrival)).values())
    assert agg.late and counted + sum(l.event.value for l in agg.late) == sum(e.value for e in ordered)


@pytest.mark.parametrize("window", [Tumbling(25), Sliding(40, 10), Session(15)], ids=["tumbling", "sliding", "session"])
def test_checkpoint_and_replay_gives_exactly_the_uninterrupted_output(window):
    _, arrival = make_stream(random.Random(2), jitter=30)
    make = lambda: WindowedAggregator(window, max_out_of_order=10, allowed_lateness=15)
    expected = make().run(arrival)

    first = make()
    before = []
    for event in arrival[:150]:
        before += first.process(event)
    snapshot = first.checkpoint()
    for event in arrival[150:230]:                      # work done after the checkpoint is lost in the crash
        first.process(event)

    recovered = make()
    recovered.restore(snapshot)
    assert recovered.offset == 150
    after = []
    for event in arrival[recovered.offset:]:            # the source replays from the saved offset
        after += recovered.process(event)
    after += recovered.flush()
    assert before + after == expected
    assert recovered.late == make_late(arrival, window)


def make_late(arrival, window):
    agg = WindowedAggregator(window, max_out_of_order=10, allowed_lateness=15)
    agg.run(arrival)
    return agg.late
