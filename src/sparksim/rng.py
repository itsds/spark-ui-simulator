"""Seeded random streams.

Each model component draws from its own named stream, derived from the
scenario seed by hashing. Adding draws to one component therefore doesn't shift
the numbers of another, and the same seed always gives the same log.
"""

from __future__ import annotations

import hashlib
import math
import random


def stream_seed(seed: int, name: str) -> int:
    return int.from_bytes(hashlib.sha256(f"{seed}/{name}".encode()).digest()[:8], "big")


def substream(seed: int, name: str) -> random.Random:
    return random.Random(stream_seed(seed, name))


def numpy_rng(seed: int, name: str):
    import numpy as np

    return np.random.Generator(np.random.PCG64(stream_seed(seed, name)))


def lognormal(rng: random.Random, sigma: float, median: float = 1.0) -> float:
    """Lognormal with the given median (not mean): median * exp(N(0, sigma))."""
    return median * math.exp(rng.gauss(0.0, sigma)) if sigma else median
