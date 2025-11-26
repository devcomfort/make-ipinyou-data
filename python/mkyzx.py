#!/usr/bin/env python3
"""Convert IPinyou dataset to LibSVM format with feature indexing."""

from __future__ import annotations

import argparse
import operator
from pathlib import Path
from typing import TextIO

OSES = ["windows", "ios", "mac", "android", "linux"]
BROWSERS = ["chrome", "sogou", "maxthon", "safari", "firefox", "theworld", "opera", "ie"]

F1S = [
    "weekday",
    "hour",
    "IP",
    "region",
    "city",
    "adexchange",
    "domain",
    "slotid",
    "slotwidth",
    "slotheight",
    "slotvisibility",
    "slotformat",
    "creative",
    "advertiser",
]

F1SP = ["useragent", "slotprice"]


def feat_trans(name: str, content: str) -> str:
    """Transform feature content based on feature name."""
    content = content.lower()
    if name == "useragent":
        operation = "other"
        for o in OSES:
            if o in content:
                operation = o
                break
        browser = "other"
        for b in BROWSERS:
            if b in content:
                browser = b
                break
        return operation + "_" + browser
    if name == "slotprice":
        try:
            price = int(content)
            if price > 100:
                return "101+"
            elif price > 50:
                return "51-100"
            elif price > 10:
                return "11-50"
            elif price > 0:
                return "1-10"
            else:
                return "0"
        except ValueError:
            return "0"
    return content


def get_tags(content: str) -> list[str]:
    """Extract tags from comma-separated content."""
    if content == "\n" or len(content) == 0:
        return ["null"]
    return content.strip().split(",")

def build_feature_index(train_path: Path) -> tuple[dict[str, int], dict[str, int]]:
    """Build feature index from training data."""
    if not train_path.exists():
        raise FileNotFoundError(f"Training file not found: {train_path}")
    if not train_path.is_file():
        raise ValueError(f"Training path is not a file: {train_path}")

    namecol: dict[str, int] = {}
    featindex: dict[str, int] = {}
    maxindex = 0

    featindex["truncate"] = maxindex
    maxindex += 1

    try:
        with train_path.open("r", encoding="utf-8") as fi:
            first = True
            for line_num, line in enumerate(fi, start=1):
                s = line.rstrip("\n").split("\t")
                if first:
                    first = False
                    if not s or not any(col.strip() for col in s):
                        raise ValueError(
                            f"Training file has empty or invalid header: {train_path}"
                        )
                    for i in range(len(s)):
                        namecol[s[i].strip()] = i
                        if i > 0:
                            featindex[str(i) + ":other"] = maxindex
                            maxindex += 1
                    continue

                for f in F1S:
                    if f not in namecol:
                        continue
                    col = namecol[f]
                    if col >= len(s):
                        continue
                    content = s[col]
                    feat = str(col) + ":" + content
                    if feat not in featindex:
                        featindex[feat] = maxindex
                        maxindex += 1

                for f in F1SP:
                    if f not in namecol:
                        continue
                    col = namecol[f]
                    if col >= len(s):
                        continue
                    content = feat_trans(f, s[col])
                    feat = str(col) + ":" + content
                    if feat not in featindex:
                        featindex[feat] = maxindex
                        maxindex += 1

                if "usertag" in namecol:
                    col = namecol["usertag"]
                    if col < len(s):
                        tags = get_tags(s[col])
                        for tag in tags:
                            feat = str(col) + ":" + tag
                            if feat not in featindex:
                                featindex[feat] = maxindex
                                maxindex += 1

        if not namecol:
            raise ValueError(f"No valid header found in training file: {train_path}")

        return namecol, featindex
    except OSError as e:
        raise RuntimeError(f"Failed to read training file {train_path}: {e}") from e


def write_feature_index(featindex: dict[str, int], output_path: Path) -> None:
    """Write feature index to file."""
    if not featindex:
        raise ValueError("Feature index is empty, cannot write to file")

    try:
        featvalue = sorted(featindex.items(), key=operator.itemgetter(1))
        with output_path.open("w", encoding="utf-8") as fo:
            for fv in featvalue:
                fo.write(f"{fv[0]}\t{fv[1]}\n")
    except OSError as e:
        raise RuntimeError(f"Failed to write feature index to {output_path}: {e}") from e


def index_file(
    input_path: Path,
    output_path: Path,
    namecol: dict[str, int],
    featindex: dict[str, int],
) -> None:
    """Convert input file to LibSVM format."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not input_path.is_file():
        raise ValueError(f"Input path is not a file: {input_path}")

    if "truncate" not in featindex:
        raise ValueError("Feature index missing required 'truncate' feature")

    try:
        with input_path.open("r", encoding="utf-8") as fi, output_path.open(
            "w", encoding="utf-8"
        ) as fo:
            first = True
            for line_num, line in enumerate(fi, start=1):
                if first:
                    first = False
                    continue

                s = line.rstrip("\n").split("\t")
                if len(s) < 24:
                    continue

                click = s[0]
                winning_price = s[23] if len(s) > 23 else "0"
                fo.write(f"{click} {winning_price}")

                # Add truncate feature
                index = featindex["truncate"]
                fo.write(f" {index}:1")

                # Process F1S features
                for f in F1S:
                    if f not in namecol:
                        continue
                    col = namecol[f]
                    if col >= len(s):
                        continue
                    content = s[col]
                    feat = str(col) + ":" + content
                    if feat not in featindex:
                        feat = str(col) + ":other"
                    if feat in featindex:
                        index = featindex[feat]
                        fo.write(f" {index}:1")

                # Process F1SP features
                for f in F1SP:
                    if f not in namecol:
                        continue
                    col = namecol[f]
                    if col >= len(s):
                        continue
                    content = feat_trans(f, s[col])
                    feat = str(col) + ":" + content
                    if feat not in featindex:
                        feat = str(col) + ":other"
                    if feat in featindex:
                        index = featindex[feat]
                        fo.write(f" {index}:1")

                # Process usertag
                if "usertag" in namecol:
                    col = namecol["usertag"]
                    if col < len(s):
                        tags = get_tags(s[col])
                        for tag in tags:
                            feat = str(col) + ":" + tag
                            if feat not in featindex:
                                feat = str(col) + ":other"
                            if feat in featindex:
                                index = featindex[feat]
                                fo.write(f" {index}:1")

                fo.write("\n")
    except OSError as e:
        raise RuntimeError(
            f"Failed to process file {input_path} or write to {output_path}: {e}"
        ) from e


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("train_log", type=Path, help="Training log file")
    parser.add_argument("test_log", type=Path, help="Test log file")
    parser.add_argument("train_lr", type=Path, help="Output training LibSVM file")
    parser.add_argument("test_lr", type=Path, help="Output test LibSVM file")
    parser.add_argument("featindex", type=Path, help="Output feature index file")
    return parser.parse_args()


def main() -> None:
    import sys

    try:
        args = parse_args()

        # Build feature index
        print("Building feature index...")
        namecol, featindex = build_feature_index(args.train_log)
        print(f"Feature size: {len(featindex)}")

        # Write feature index
        write_feature_index(featindex, args.featindex)

        # Index training file
        print(f"Indexing {args.train_log}")
        index_file(args.train_log, args.train_lr, namecol, featindex)

        # Index test file
        print(f"Indexing {args.test_log}")
        index_file(args.test_log, args.test_lr, namecol, featindex)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
