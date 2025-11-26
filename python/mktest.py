#!/usr/bin/env python3
"""Add click, weekday, and hour columns to IPinyou test dataset."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import TextIO


def load_schema(schema_path: Path) -> list[str]:
    """Load schema from file and append additional columns."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    if not schema_path.is_file():
        raise ValueError(f"Schema path is not a file: {schema_path}")

    try:
        with schema_path.open("r", encoding="utf-8") as f:
            schema = [s.strip() for s in f.read().split()]
            if not schema:
                raise ValueError(f"Schema file is empty: {schema_path}")
            schema += ["nclick", "nconversation"]
            return schema
    except OSError as e:
        raise RuntimeError(f"Failed to read schema file {schema_path}: {e}") from e


def process_data(schema: list[str], fi: TextIO, fo: TextIO) -> None:
    """Process input data and add click, weekday, hour columns."""
    try:
        timestamp_index = schema.index("timestamp")
    except ValueError:
        raise ValueError("Required field 'timestamp' not found in schema")

    # Write header
    fo.write("click\tweekday\thour\t" + "\t".join(schema) + "\n")

    for line_num, line in enumerate(fi, start=1):
        arr = line.rstrip("\n").split("\t")
        if len(arr) < 2:
            continue

        bid = arr[0] + "-" + arr[1]
        click = "0" if len(arr) >= 2 and arr[-2] == "0" else "1"

        if len(arr) <= timestamp_index:
            continue

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
    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        schema = load_schema(args.schema)
        process_data(schema, sys.stdin, sys.stdout)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
