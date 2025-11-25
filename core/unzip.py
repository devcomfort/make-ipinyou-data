from pathlib import Path
from typing import List
import bz2

from tqdm import tqdm


def unzip_bz2(path: Path | List[Path]) -> None:
    if isinstance(path, Path):
        path = [path]
    for p in path:
        with bz2.open(p, "rb") as f:
            with open(p.with_suffix(""), "wb") as fout:
                fout.write(f.read())
