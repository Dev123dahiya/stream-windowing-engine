"""Demo: python -m streamwin

Simulated page views (random, seeded) arrive out of order. Shows what the
out-of-order bound buys, and what it costs when set too low.
"""
from __future__ import annotations

import random

from . import Event, Tumbling, WindowedAggregator, batch_reference
from .engine import final_results


def simulate(n: int = 5000, seed: int = 7) -> tuple[list[Event], list[Event]]:
    """Events happen in order; most arrive within 2 s, 5% are delayed by up to 60 s (a phone coming back online)."""
    rng = random.Random(seed)
    happened = [Event(rng.choice(["/home", "/pricing", "/docs", "/login"]), ts) for ts in sorted(rng.randrange(3600) for _ in range(n))]
    delay = lambda: rng.uniform(0, 60) if rng.random() < 0.05 else rng.uniform(0, 2)
    return happened, sorted(happened, key=lambda e: e.ts + delay())


def main() -> None:
    happened, arrived = simulate()
    window = Tumbling(60)
    truth = batch_reference(happened, window)
    behind, newest = 0, -1
    for e in arrived:
        behind += e.ts < newest
        newest = max(newest, e.ts)
    print(f"{len(arrived):,} page views over one hour, counted per page per minute ({len(truth)} windows).")
    print(f"{behind:,} events arrived after a newer one had already been seen.\n")
    print("| Out-of-order bound | Allowed lateness | Windows right first time | Corrections sent | Events dropped as late | Final answer matches batch |")
    print("|---:|---:|---:|---:|---:|---|")
    for bound, lateness in [(0, 0), (5, 0), (60, 0), (5, 60), (0, 120)]:
        agg = WindowedAggregator(window, max_out_of_order=bound, allowed_lateness=lateness)
        results = agg.run(arrived)
        first = {(r.key, r.start, r.end): r.value for r in results if r.revision == 0}
        right_first = sum(1 for tag, v in first.items() if truth.get(tag) == v)
        print(f"| {bound} s | {lateness} s | {right_first / len(truth):.1%} | {sum(1 for r in results if r.revision > 0)} | "
              f"{len(agg.late)} | {'yes' if final_results(results) == truth else 'no'} |")
    print("\nA larger bound waits longer before answering. Allowed lateness answers early and corrects afterwards.")


if __name__ == "__main__":
    main()
