#!/usr/bin/env python3
"""Normalize user-agent fields inside an IPinyou dataset file."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

OSES: tuple[str, ...] = ("windows", "ios", "mac", "android", "linux")
BROWSERS: tuple[str, ...] = (
    "chrome",
    "sogou",
    "maxthon",
    "safari",
    "firefox",
    "theworld",
    "opera",
    "ie",
)


def detect_keyword(ua: str, candidates: Iterable[str]) -> str:
    """Return the first candidate contained in the UA string, fallback to other."""
    for candidate in candidates:
        if candidate in ua:
            return candidate
    return "other"


def normalize_fields(fields: list[str]) -> list[str]:
    """Replace empty strings with null, preserving original newline semantics."""
    normalized = []
    for value in fields:
        normalized.append(value if value else "null")
    return normalized


def process_file(path: Path) -> None:
    """Process file to normalize user-agent fields."""
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")

    temp_path = path.with_suffix(path.suffix + ".fmua")
    try:
        with path.open("r", encoding="utf-8") as fi, temp_path.open(
            "w", encoding="utf-8"
        ) as fo:
            for idx, line in enumerate(fi):
                if idx == 0:
                    if not line.strip():
                        raise ValueError("Input file is empty or has no header")
                    fo.write(line)
                    continue

                has_newline = line.endswith("\n")
                fields = line.rstrip("\n").split("\t")

                ua = fields[7].lower() if len(fields) > 7 else ""
                operation = detect_keyword(ua, OSES)
                browser = detect_keyword(ua, BROWSERS)
                fmua = f"{operation}_{browser}"

                normalized = normalize_fields(fields)
                if len(normalized) > 7:
                    normalized[7] = fmua
                else:
                    normalized.extend(["null"] * (8 - len(normalized)))
                    normalized[7] = fmua

                fo.write("\t".join(normalized))
                if has_newline:
                    fo.write("\n")

        os.rename(temp_path, path)
    except OSError as e:
        if temp_path.exists():
            temp_path.unlink()
        raise RuntimeError(f"Failed to process file {path}: {e}") from e


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", type=Path, help="Dataset file to normalize")
    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        process_file(args.input_path)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
