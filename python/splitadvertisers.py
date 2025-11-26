#!/usr/bin/env python3
"""Split IPinyou dataset files by advertiser into separate directories."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TextIO


def split_file_by_advertiser(
    input_path: Path, output_folder: Path, advertiser_index: int
) -> None:
    """Split input file by advertiser into separate files."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not input_path.is_file():
        raise ValueError(f"Input path is not a file: {input_path}")

    if advertiser_index < 0:
        raise ValueError(f"Invalid advertiser_index: {advertiser_index} (must be >= 0)")

    advertiser_files: dict[str, TextIO] = {}
    header = ""

    try:
        with input_path.open("r", encoding="utf-8") as fi:
            first = True
            for line_num, line in enumerate(fi, start=1):
                if first:
                    first = False
                    header = line
                    if not header.strip():
                        raise ValueError(
                            f"Input file has empty header: {input_path}"
                        )
                    continue

                fields = line.rstrip("\n").split("\t")
                if advertiser_index >= len(fields):
                    continue

                advertiser = fields[advertiser_index]
                if not advertiser:
                    continue

                if advertiser not in advertiser_files:
                    advertiser_dir = output_folder / advertiser
                    try:
                        advertiser_dir.mkdir(parents=True, exist_ok=True)
                    except OSError as e:
                        raise RuntimeError(
                            f"Failed to create directory {advertiser_dir}: {e}"
                        ) from e

                    output_file = advertiser_dir / input_path.name
                    try:
                        advertiser_files[advertiser] = output_file.open(
                            "w", encoding="utf-8"
                        )
                        advertiser_files[advertiser].write(header)
                    except OSError as e:
                        raise RuntimeError(
                            f"Failed to create output file {output_file}: {e}"
                        ) from e

                advertiser_files[advertiser].write(line)

        # Close all files
        for fo in advertiser_files.values():
            fo.close()
    except OSError as e:
        # Close any open files before raising
        for fo in advertiser_files.values():
            try:
                fo.close()
            except Exception:
                pass
        raise RuntimeError(f"Failed to process file {input_path}: {e}") from e


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output_folder",
        type=Path,
        help="Output folder to create advertiser subdirectories",
    )
    parser.add_argument(
        "advertiser_index",
        type=int,
        help="Column index of advertiser field (0-based)",
    )
    parser.add_argument(
        "input_files",
        nargs="+",
        type=Path,
        help="Input dataset files to split",
    )
    return parser.parse_args()


def main() -> None:
    import sys

    try:
        args = parse_args()

        if args.advertiser_index < 0:
            raise ValueError(
                f"Invalid advertiser_index: {args.advertiser_index} (must be >= 0)"
            )

        for input_file in args.input_files:
            split_file_by_advertiser(
                input_file, args.output_folder, args.advertiser_index
            )
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
