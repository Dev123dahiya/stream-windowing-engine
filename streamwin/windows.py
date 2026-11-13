"""Window assigners: which window(s) does an event with this timestamp belong to?"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Tumbling:
    """Fixed, non-overlapping windows: [0, size), [size, 2*size), ..."""

    size: int
    merging = False

    def assign(self, ts: int) -> list[tuple[int, int]]:
        start = ts - ts % self.size
        return [(start, start + self.size)]


@dataclass(frozen=True)
class Sliding:
    """Windows of ``size`` that start every ``slide``; an event falls in size/slide of them."""

    size: int
    slide: int
    merging = False

    def __post_init__(self) -> None:
        if self.slide <= 0 or self.size < self.slide:
            raise ValueError("need 0 < slide <= size")

    def assign(self, ts: int) -> list[tuple[int, int]]:
        last_start = ts - ts % self.slide
        starts = range(last_start, ts - self.size, -self.slide)
        return [(s, s + self.size) for s in reversed(starts)]


@dataclass(frozen=True)
class Session:
    """Windows that close after ``gap`` time units of silence for a key.

    Each event starts as its own window [ts, ts + gap). Windows that overlap or
    touch are merged, so a late event can bridge two sessions into one.
    """

    gap: int
    merging = True

    def assign(self, ts: int) -> list[tuple[int, int]]:
        return [(ts, ts + self.gap)]
