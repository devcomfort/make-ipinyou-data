from __future__ import annotations

from pathlib import Path
from typing import Sequence


def ensure_sequence(path: Path | Sequence[Path]) -> list[Path]:
    """Coerce an input path or collection into a list.

    Parameters
    ----------
    path : Path or Sequence[Path]
        Single path or collection of paths pointing to bz2 archives.

    Returns
    -------
    list[Path]
        Normalized list of ``Path`` objects.
    """
    if isinstance(path, Path):
        return [path]
    return [Path(p) for p in path]

