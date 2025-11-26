from __future__ import annotations

from pathlib import Path
from typing import Iterable


def filter_bz2(paths: Iterable[Path]) -> tuple[list[Path], list[Path]]:
    """Split incoming paths into valid bz2 inputs and rejected entries.

    Parameters
    ----------
    paths : Iterable[Path]
        Candidate paths.

    Returns
    -------
    tuple[list[Path], list[Path]]
        First element contains valid ``.bz2`` files, second contains rejections.
    """
    bz2_files: list[Path] = []
    rejected: list[Path] = []
    for candidate in paths:
        candidate = Path(candidate)
        if candidate.suffix.lower() != ".bz2" or not candidate.exists():
            rejected.append(candidate)
            continue
        bz2_files.append(candidate)
    return bz2_files, rejected

