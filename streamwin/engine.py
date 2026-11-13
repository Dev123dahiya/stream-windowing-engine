from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable, Hashable, Iterable


@dataclass(frozen=True)
class Event:
    key: Hashable
    ts: int          # event time: when it happened, not when it arrived
    value: Any = 1


@dataclass(frozen=True)
class Result:
    key: Hashable
    start: int
    end: int
    value: Any
    revision: int = 0    # 0 is the first result for a window; later ones correct it after late data


@dataclass(frozen=True)
class Late:
    """An event that arrived after its window was closed for good. Routed aside, never silently dropped."""

    event: Event
    watermark: int


class WindowedAggregator:
    """Aggregate events per key and window in event time.

    Events may arrive out of order. The operator tracks a **watermark**: its
    estimate that no event older than that time is still on its way, computed as
    (highest timestamp seen) - ``max_out_of_order``. A window's result is emitted
    once the watermark passes the window's end.

    Late events, those behind the watermark, are still accepted for
    ``allowed_lateness`` after their window closed and produce a corrected
    result (a higher ``revision``). After that the window's state is deleted and
    any straggler goes to the ``late`` list.

    ``add`` folds one value into an accumulator and ``merge`` combines two
    accumulators (needed for session windows). They must be associative and
    commutative, or results would depend on arrival order.
    """

    def __init__(self, window, *, init: Callable[[], Any] = lambda: 0, add: Callable[[Any, Any], Any] = lambda acc, v: acc + v,
                 merge: Callable[[Any, Any], Any] | None = None, max_out_of_order: int = 0, allowed_lateness: int = 0) -> None:
        self.window, self.init, self.add = window, init, add
        self.merge = merge or (lambda a, b: a + b)
        self.max_out_of_order, self.allowed_lateness = max_out_of_order, allowed_lateness
        self.watermark = float("-inf")
        self._max_ts = float("-inf")
        self._state: dict[Hashable, dict[tuple[int, int], Any]] = {}      # key -> window -> accumulator
        self._emitted: dict[tuple[Hashable, int, int], int] = {}          # (key, start, end) -> last revision sent
        self.late: list[Late] = []
        self.offset = 0                                                   # events consumed, for checkpoints

    # ---- ingest ----------------------------------------------------------
    def process(self, event: Event) -> list[Result]:
        """Consume one event; return any results it caused to be emitted."""
        self.offset += 1
        out: list[Result] = []
        windows = self.window.assign(event.ts)
        alive = [w for w in windows if w[1] + self.allowed_lateness > self.watermark]
        if not alive:
            self.late.append(Late(event, int(self.watermark)))
        else:
            per_key = self._state.setdefault(event.key, {})
            for w in alive:
                if self.window.merging:
                    w, acc = self._merge_into(event.key, per_key, w)
                    per_key[w] = self.add(acc, event.value)
                else:
                    per_key[w] = self.add(per_key[w] if w in per_key else self.init(), event.value)
                if w[1] <= self.watermark:
                    # The window already fired: this is late-but-allowed data, so send a correction now.
                    out.append(self._emit(event.key, w, per_key[w]))
        if event.ts > self._max_ts:
            self._max_ts = event.ts
            out.extend(self.advance_watermark(event.ts - self.max_out_of_order))
        return out

    def _merge_into(self, key, per_key: dict, new: tuple[int, int]):
        """Merge a new session window with every existing one it overlaps or touches."""
        start, end = new
        acc = self.init()
        for w in sorted(per_key):
            if w[0] <= end and start <= w[1]:
                start, end = min(start, w[0]), max(end, w[1])
                acc = self.merge(acc, per_key.pop(w))
                # A merged-away window that had already fired is superseded; forget its emission record.
                self._emitted.pop((key, w[0], w[1]), None)
        return (start, end), acc

    def advance_watermark(self, watermark: float) -> list[Result]:
        """Move the watermark forward: fire windows it has passed and drop expired state."""
        if watermark <= self.watermark:
            return []
        self.watermark = watermark
        out: list[Result] = []
        for key in list(self._state):
            per_key = self._state[key]
            for w in sorted(per_key):
                if w[1] <= watermark and (key, w[0], w[1]) not in self._emitted:
                    out.append(self._emit(key, w, per_key[w]))
                if w[1] + self.allowed_lateness <= watermark:
                    del per_key[w]                      # no more corrections possible: free the memory
                    self._emitted.pop((key, w[0], w[1]), None)
            if not per_key:
                del self._state[key]
        return sorted(out, key=lambda r: (r.end, r.start, str(r.key)))

    def flush(self) -> list[Result]:
        """End of stream: nothing else is coming, so close every open window."""
        return self.advance_watermark(float("inf"))

    def _emit(self, key, window, value) -> Result:
        tag = (key, window[0], window[1])
        revision = self._emitted[tag] + 1 if tag in self._emitted else 0
        self._emitted[tag] = revision
        return Result(key, window[0], window[1], copy.deepcopy(value), revision)

    def run(self, events: Iterable[Event]) -> list[Result]:
        out: list[Result] = []
        for event in events:
            out.extend(self.process(event))
        out.extend(self.flush())
        return out

    # ---- state -----------------------------------------------------------
    @property
    def open_windows(self) -> int:
        return sum(len(v) for v in self._state.values())

    def checkpoint(self) -> dict:
        """Snapshot everything needed to resume: state, watermark and the input offset.

        Restoring this and replaying the input from ``offset`` reproduces the
        same results as an uninterrupted run. Because state and offset are saved
        together, no event is counted twice or skipped.
        """
        return copy.deepcopy({"offset": self.offset, "watermark": self.watermark, "max_ts": self._max_ts,
                              "state": self._state, "emitted": self._emitted, "late": self.late})

    def restore(self, snapshot: dict) -> None:
        snapshot = copy.deepcopy(snapshot)
        self.offset, self.watermark, self._max_ts = snapshot["offset"], snapshot["watermark"], snapshot["max_ts"]
        self._state, self._emitted, self.late = snapshot["state"], snapshot["emitted"], snapshot["late"]


def final_results(results: list[Result]) -> dict[tuple, Any]:
    """Collapse revisions: the latest value for each (key, start, end)."""
    latest: dict[tuple, Result] = {}
    for r in results:
        tag = (r.key, r.start, r.end)
        if tag not in latest or r.revision >= latest[tag].revision:
            latest[tag] = r
    return {tag: r.value for tag, r in latest.items()}


def batch_reference(events: list[Event], window, init=lambda: 0, add=lambda acc, v: acc + v) -> dict[tuple, Any]:
    """The answer a batch job would give with all the data in hand. Used to check the streaming result."""
    out: dict[tuple, Any] = {}
    if window.merging:
        by_key: dict[Hashable, list[Event]] = {}
        for e in events:
            by_key.setdefault(e.key, []).append(e)
        for key, items in by_key.items():
            items.sort(key=lambda e: e.ts)
            start, end, acc = None, None, None
            for e in items:
                if start is None or e.ts > end:
                    if start is not None:
                        out[(key, start, end)] = acc
                    start, end, acc = e.ts, e.ts + window.gap, init()
                end = max(end, e.ts + window.gap)
                acc = add(acc, e.value)
            out[(key, start, end)] = acc
        return out
    for e in events:
        for w in window.assign(e.ts):
            tag = (e.key, w[0], w[1])
            out[tag] = add(out[tag] if tag in out else init(), e.value)
    return out
