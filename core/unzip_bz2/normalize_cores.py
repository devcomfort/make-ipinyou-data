from __future__ import annotations

import os


def normalize_cores(requested: int | None) -> int:
    """Normalize the requested worker count.

    Parameters
    ----------
    requested : int or None
        Desired number of worker threads. ``None`` means use all CPUs.

    Returns
    -------
    int
        A safe value between 1 and ``os.cpu_count() - 1`` (inclusive).
    """
    cpu_total = os.cpu_count() or 1
    if requested is None:
        return cpu_total
    upper_bound = max(1, cpu_total - 1)
    return max(1, min(int(requested), upper_bound))

