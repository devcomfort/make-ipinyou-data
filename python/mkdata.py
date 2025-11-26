#!/usr/bin/env python3
"""Add click, weekday, and hour columns to IPinyou dataset based on click files."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import TextIO


def load_schema(schema_path: Path) -> list[str]:
    """Load schema from file."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    if not schema_path.is_file():
        raise ValueError(f"Schema path is not a file: {schema_path}")

    try:
        with schema_path.open("r", encoding="utf-8") as f:
            schema = [s.strip() for s in f.read().split()]
            if not schema:
                raise ValueError(f"Schema file is empty: {schema_path}")
            return schema
    except OSError as e:
        raise RuntimeError(f"Failed to read schema file {schema_path}: {e}") from e


def build_click_map(click_files: list[Path], creative_index: int) -> dict[str, int]:
    """Build a map of bid-creative pairs to line counts from click files."""
    if creative_index < 0:
        raise ValueError(f"Invalid creative_index: {creative_index}")

    bmap: dict[str, int] = {}
    for fn in click_files:
        if not fn.exists():
            raise FileNotFoundError(f"Click file not found: {fn}")
        if not fn.is_file():
            raise ValueError(f"Click path is not a file: {fn}")

        try:
            with fn.open("r", encoding="utf-8") as f:
                for lcnt, line in enumerate(f):
                    arr = line.rstrip("\n").split("\t")
                    if len(arr) > creative_index:
                        bid = arr[0] + "-" + arr[creative_index]
                        bmap[bid] = lcnt
        except OSError as e:
            raise RuntimeError(f"Failed to read click file {fn}: {e}") from e

    return bmap


def process_data(
    schema: list[str], bmap: dict[str, int], fi: TextIO, fo: TextIO
) -> None:
    """Process input data and add click, weekday, hour columns."""
    try:
        timestamp_index = schema.index("timestamp")
    except ValueError:
        raise ValueError("Required field 'timestamp' not found in schema")

    try:
        creative_index = schema.index("creative")
    except ValueError:
        raise ValueError("Required field 'creative' not found in schema")

    # Write header
    fo.write("click\tweekday\thour\t" + "\t".join(schema) + "\n")

    for line_num, line in enumerate(fi, start=1):
        arr = line.rstrip("\n").split("\t")
        if len(arr) <= max(timestamp_index, creative_index):
            continue

        bid = arr[0] + "-" + arr[creative_index]
        click = "1" if bid in bmap else "0"

        ts = arr[timestamp_index]
        if len(ts) < 8:
            raise ValueError(
                f"Invalid timestamp format at line {line_num}: '{ts}' (expected at least 8 characters)"
            )

        try:
            year = int(ts[0:4])
            month = int(ts[4:6])
            day = int(ts[6:8])
            d = date(year, month, day)
            weekday = int(d.strftime("%w"))
            hour = ts[8:10] if len(ts) > 10 else "00"
        except (ValueError, IndexError) as e:
            raise ValueError(
                f"Failed to parse timestamp at line {line_num}: '{ts}' - {e}"
            ) from e

        fo.write(f"{click}\t{weekday}\t{hour}\t{line}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schema", type=Path, help="Schema file")
    parser.add_argument("clickfiles", nargs="+", type=Path, help="Click files")
    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        schema = load_schema(args.schema)
        creative_index = schema.index("creative")
        bmap = build_click_map(args.clickfiles, creative_index)
        process_data(schema, bmap, sys.stdin, sys.stdout)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
