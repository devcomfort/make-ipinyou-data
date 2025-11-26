from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

from loguru import logger
from tqdm import tqdm

from .decompress_file import decompress_file
from .ensure_sequence import ensure_sequence
from .filter_bz2 import filter_bz2
from .normalize_cores import normalize_cores


def unzip_bz2(
    path: Path | List[Path], cores: int | None = None, verbose: bool = False
) -> list[Path]:
    """Decompress multiple bz2 archives concurrently.

    Parameters
    ----------
    path : Path or list[Path]
        Single path or iterable of paths pointing to bz2 files.
    cores : int or None, optional
        Desired worker count; normalized to ``[1, os.cpu_count()-1]``.
    verbose : bool, optional
        Enable detailed logging and progress bars when ``True``.

    Returns
    -------
    list[Path]
        Paths to the extracted files.
    """
    normalized_paths = ensure_sequence(path)
    bz2_files, rejected = filter_bz2(normalized_paths)

    if verbose and rejected:
        logger.warning(
            "%d inputs were filtered out (non-bz2 or missing)", len(rejected)
        )

    if not bz2_files:
        if verbose:
            logger.warning("No .bz2 files to decompress")
        return []

    worker_count = normalize_cores(cores)
    if verbose:
        logger.info(
            "Using %d worker threads to decompress %d files",
            worker_count,
            len(bz2_files),
        )

    completed: list[Path] = []
    progress = tqdm(
        total=len(bz2_files), disable=not verbose, desc="unzip bz2", unit="file"
    )
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(decompress_file, src, verbose): src for src in bz2_files
        }
        for future in as_completed(futures):
            completed.append(future.result())
            progress.update(1)
    progress.close()

    if verbose:
        logger.info("%d files were skipped by the filter", len(rejected))

    return completed
