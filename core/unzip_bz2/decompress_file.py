from __future__ import annotations

import bz2
import shutil
from pathlib import Path

from loguru import logger


def decompress_file(src: Path, verbose: bool) -> Path:
    """Decompress a single bz2 archive.

    Parameters
    ----------
    src : Path
        Source bz2 archive.
    verbose : bool
        Whether to log progress messages.

    Returns
    -------
    Path
        Destination path of the extracted file.
    """
    dest = src.with_suffix("")
    if verbose:
        logger.info("Start decompressing %s -> %s", src.name, dest.name)
    with bz2.open(src, "rb") as src_fh, open(dest, "wb") as dest_fh:
        shutil.copyfileobj(src_fh, dest_fh)
    if verbose:
        logger.success("Finished decompressing %s", src.name)
    return dest
