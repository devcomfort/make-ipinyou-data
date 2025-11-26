#!/usr/bin/env python3
"""DuckDB 기반 iPinYou 데이터 파이프라인.

기존 Makefile 파이프라인(init → clk → train.log → test.log → advertisers → yzx)을
DuckDB 하나로 대체하여, 헤더를 포함한 TSV를 바로 생성한다.

주요 기능:
- `imp.*.txt(.bz2)` 및 `clk.*.txt(.bz2)` 원시 로그를 DuckDB로 직접 로딩
- 클릭 여부, 요일, 시간 파생 컬럼 계산
- User-Agent를 OS/브라우저 조합으로 축약(fmua)
- 문자열 공백/빈값을 "null"로 정규화
- 학습/테스트 TSV 및 (옵션) 광고주별 TSV 출력
"""

from __future__ import annotations

import bz2
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import duckdb
from dotenv import load_dotenv
from loguru import logger
from ua_parser import user_agent_parser

BASE_SCHEMA = [
    "bidid",
    "timestamp",
    "logtype",
    "ipinyouid",
    "useragent",
    "IP",
    "region",
    "city",
    "adexchange",
    "domain",
    "url",
    "urlid",
    "slotid",
    "slotwidth",
    "slotheight",
    "slotvisibility",
    "slotformat",
    "slotprice",
    "creative",
    "bidprice",
    "payprice",
    "keypage",
    "advertiser",
    "usertag",
]

TEST_ADDITIONAL_COLUMNS = ["nclick", "nconversation"]


def read_schema(schema_path: Path) -> list[str]:
    if not schema_path.exists():
        raise FileNotFoundError(f"스키마 파일을 찾을 수 없습니다: {schema_path}")
    columns: list[str] = []
    for raw in schema_path.read_text(encoding="utf-8").splitlines():
        col = raw.strip()
        if col:
            columns.append(col)
    if not columns:
        raise ValueError(f"스키마 파일이 비어 있습니다: {schema_path}")
    return columns


def collect_files(directory: Path, pattern: str) -> list[Path]:
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"{directory} 에서 패턴 {pattern} 파일을 찾지 못했습니다."
        )
    return files


def resolve_round_dirs(dataset_root: Path, names: list[str], kind: str) -> list[Path]:
    dirs: list[Path] = []
    for name in names:
        path = dataset_root / name
        if not path.is_dir():
            raise FileNotFoundError(
                f"{kind} 라운드 '{name}' 디렉터리를 찾지 못했습니다: {path}"
            )
        dirs.append(path)
    return dirs


def quote_ident(identifier: str) -> str:
    """DuckDB 식별자 이스케이프."""
    return '"' + identifier.replace('"', '""') + '"'


def quote_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def configure_logging(verbose: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO" if verbose else "WARNING")


def env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value).expanduser() if value else default


def env_int(name: str, default: int | None) -> int | None:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("환경 변수 %s=%s 는 정수가 아님, 기본값 사용", name, value)
        return default


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_str(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value else None


def normalize_family(value: str | None) -> str:
    if not value:
        return "other"
    cleaned = value.strip().lower().replace(" ", "_")
    cleaned = "".join(ch for ch in cleaned if ch.isalnum() or ch in {"_", "-"})
    return cleaned or "other"


def ua_signature(ua: str | None) -> str:
    if not ua:
        return "other_other"
    try:
        parsed = user_agent_parser.Parse(ua)
    except Exception:
        return "other_other"
    os_family = normalize_family(parsed.get("os", {}).get("family"))
    ua_family = normalize_family(parsed.get("user_agent", {}).get("family"))
    return f"{os_family}_{ua_family}"


def parse_rounds(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


def format_duration(seconds: float) -> str:
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class ProgressTracker:
    def __init__(self, total_steps: int, enabled: bool):
        self.enabled = enabled
        self.total_steps = total_steps
        self.current_step = 0
        self.start_time = time.time()

    def step(self, message: str) -> None:
        self.current_step += 1
        if not self.enabled:
            return
        elapsed = format_duration(time.time() - self.start_time)
        logger.info(
            "[{}/{} | {}] {}",
            self.current_step,
            self.total_steps,
            elapsed,
            message,
        )

    def finish(self) -> None:
        if not self.enabled:
            return
        duration = format_duration(time.time() - self.start_time)
        logger.info(
            "[진행도] 모든 단계 완료 ({} 단계, {} 소요)", self.total_steps, duration
        )


def decompress_one(src: Path, dest: Path, verbose: bool = False) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        logger.info("decompress {} -> {}", src.name, dest.name)
    with bz2.open(src, "rb") as src_fh, open(dest, "wb") as dest_fh:
        shutil.copyfileobj(src_fh, dest_fh)
    if verbose:
        logger.success("finished {}", dest.name)
    return dest


def ensure_uncompressed(
    files: Sequence[Path],
    *,
    dataset_root: Path,
    cache_dir: Path,
    workers: int | None,
    force: bool,
    verbose: bool,
) -> list[Path]:
    pending: list[tuple[Path, Path]] = []
    prepared: list[Path] = []
    for src in files:
        if src.suffix == ".bz2":
            try:
                rel = src.relative_to(dataset_root)
            except ValueError:
                rel = Path(src.name)
            dest = cache_dir / rel.with_suffix("")
            if force or not dest.exists():
                pending.append((src, dest))
            prepared.append(dest)
        else:
            prepared.append(src)

    if pending:
        worker_count = workers or max(1, (os.cpu_count() or 2) - 1)
        if verbose:
            logger.info(
                "decompressing {} bz2 files with {} workers", len(pending), worker_count
            )
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(decompress_one, src, dest, verbose): (src, dest)
                for src, dest in pending
            }
            for future in as_completed(futures):
                future.result()
    else:
        if verbose:
            logger.info("no bz2 decompression needed")

    return prepared


def format_columns_mapping(columns: Sequence[str]) -> str:
    return ", ".join(f"'{col}':'VARCHAR'" for col in columns)


def join_file_list(files: Sequence[Path]) -> str:
    escaped = [file.as_posix().replace("'", "''") for file in files]
    return ", ".join(f"'{path}'" for path in escaped)


def register_impressions(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    files: Sequence[Path],
    columns: Sequence[str],
    round_name: str,
) -> None:
    logger.info(
        "registering impressions table={} files={} (round={} strict=False null_padding=True)",
        table_name,
        len(files),
        round_name,
    )
    file_list = join_file_list(files)
    columns_clause = format_columns_mapping(columns)
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            *,
            concat_ws('-', bidid, creative) AS bid_creative_key
            , '{round_name}'::VARCHAR AS round_name
        FROM read_csv(
            [{file_list}],
            delim='\t',
            header=FALSE,
            columns={{ {columns_clause} }},
            nullstr='',
            all_varchar=TRUE,
            auto_detect=FALSE,
            sample_size=-1,
            compression='auto',
            strict_mode=FALSE,
            null_padding=TRUE
        );
        """
    )


def register_clicks(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    files: Sequence[Path],
    columns: Sequence[str],
    round_name: str,
) -> None:
    logger.info(
        "registering clicks table={} files={} (round={} strict=False null_padding=True)",
        table_name,
        len(files),
        round_name,
    )
    file_list = join_file_list(files)
    columns_clause = format_columns_mapping(columns)
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT DISTINCT concat_ws('-', bidid, creative) AS bid_creative_key, '{round_name}'::VARCHAR AS round_name
        FROM read_csv(
            [{file_list}],
            delim='\t',
            header=FALSE,
            columns={{ {columns_clause} }},
            nullstr='',
            all_varchar=TRUE,
            auto_detect=FALSE,
            sample_size=-1,
            compression='auto',
            strict_mode=FALSE,
            null_padding=TRUE
        );
        """
    )


def build_processed_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    impressions_table: str,
    columns: Sequence[str],
    click_table: str | None,
    nclick_column: str | None,
) -> None:
    logger.info("building processed table {}", table_name)
    ts_col = f"i.{quote_ident('timestamp')}"
    parsed_ts = f"try_strptime({ts_col}, '%Y%m%d%H%M%S')"
    weekday_expr = f"COALESCE(CAST(strftime('%w', {parsed_ts}) AS VARCHAR), '0')"
    hour_expr = (
        f"CASE WHEN length({ts_col}) >= 10 THEN substr({ts_col}, 9, 2) ELSE '00' END"
    )

    if click_table:
        click_expr = "CASE WHEN c.bid_creative_key IS NULL THEN '0' ELSE '1' END"
        join_clause = f"LEFT JOIN {click_table} c USING (bid_creative_key)"
    elif nclick_column:
        nclick_col = f"i.{quote_ident(nclick_column)}"
        click_expr = (
            f"CASE WHEN NULLIF(TRIM({nclick_col}), '') = '0' THEN '0' ELSE '1' END"
        )
        join_clause = ""
    else:
        raise ValueError("click_table 또는 nclick_column 중 하나는 반드시 필요합니다.")

    fmua_expr = "ua_signature(i.useragent)"

    select_columns: list[str] = [
        f"{click_expr} AS click",
        f"{weekday_expr} AS weekday",
        f"{hour_expr} AS hour",
        "i.round_name AS round",
    ]

    for col in columns:
        ident = f"i.{quote_ident(col)}"
        if col == "useragent":
            expr = fmua_expr
        else:
            expr = f"COALESCE(NULLIF(TRIM({ident}), ''), 'null')"
        select_columns.append(f"{expr} AS {quote_ident(col)}")

    projection = ",\n        ".join(select_columns)
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
        {projection}
        FROM {impressions_table} i
        {join_clause};
        """
    )


def export_relation_to_tsv(
    rel: duckdb.DuckDBPyRelation,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("writing TSV -> {}", output_path)
    rel.write_csv(str(output_path), sep="\t", header=True)


def export_table(
    conn: duckdb.DuckDBPyConnection, table_name: str, output_path: Path
) -> None:
    export_relation_to_tsv(conn.sql(f"SELECT * FROM {table_name}"), output_path)


def export_advertisers(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    advertiser_column: str,
    output_dir: Path,
    filename: str,
) -> None:
    logger.info("exporting advertisers for {} into {}", table_name, output_dir)
    advertisers = conn.sql(
        f"SELECT DISTINCT {quote_ident(advertiser_column)} FROM {table_name}"
    ).fetchall()
    for (advertiser,) in advertisers:
        value = advertiser or "unknown"
        safe = value.replace("/", "_")
        value_literal = quote_literal(value)
        rel = conn.sql(
            f"SELECT * FROM {table_name} WHERE {quote_ident(advertiser_column)} = {value_literal}"
        )
        export_path = output_dir / safe / filename
        export_relation_to_tsv(rel, export_path)


def main() -> None:
    load_dotenv()

    verbose = env_bool("IPINYOU_VERBOSE", False)
    configure_logging(verbose)

    progress_flag = env_bool("IPINYOU_PROGRESS", False)
    force_decompress = env_bool("IPINYOU_FORCE_DECOMPRESS", False)
    split_advertisers = env_bool("IPINYOU_SPLIT_ADVERTISERS", False)

    dataset_root = env_path(
        "IPINYOU_DATASET_ROOT", Path("original-data/ipinyou.contest.dataset")
    )
    schema_file = env_path("IPINYOU_SCHEMA_FILE", Path("schema.txt"))
    output_dir = env_path("IPINYOU_OUTPUT_DIR", Path("all_duckdb"))
    database_path_env = env_str("IPINYOU_DUCKDB_FILE")
    database_path = Path(database_path_env).expanduser() if database_path_env else None
    threads = env_int("IPINYOU_DUCKDB_THREADS", None)
    memory_limit = env_str("IPINYOU_DUCKDB_MEMORY_LIMIT")
    decompressed_dir = env_path("IPINYOU_DECOMPRESSED_DIR", output_dir / "decompressed")
    decompress_workers = env_int("IPINYOU_DECOMPRESS_WORKERS", None)
    advertiser_column = env_str("IPINYOU_ADVERTISER_COLUMN") or "advertiser"

    train_output = env_path("IPINYOU_TRAIN_OUTPUT", output_dir / "train.tsv")
    test_output = env_path("IPINYOU_TEST_OUTPUT", output_dir / "test.tsv")
    pipeline_start = time.time()

    base_steps = 10
    total_steps = base_steps + (2 if split_advertisers else 0)
    progress = ProgressTracker(total_steps, progress_flag)

    base_schema = read_schema(schema_file)
    if base_schema != BASE_SCHEMA:
        logger.warning(
            "schema.txt 내용이 예상과 다릅니다. 파이프라인이 올바르지 않을 수 있습니다."
        )

    logger.info("dataset root: {}", dataset_root)

    train_dirs = sorted(dataset_root.glob("training*"))
    test_dirs = sorted(dataset_root.glob("testing*"))
    if not train_dirs or not test_dirs:
        raise FileNotFoundError(
            f"{dataset_root} 하위에서 training*/testing* 디렉터리를 찾지 못했습니다."
        )

    conn = duckdb.connect(
        database=":memory:" if database_path is None else str(database_path)
    )
    if threads:
        conn.execute(f"PRAGMA threads={threads}")
    if memory_limit:
        logger.info("Setting DuckDB memory_limit=%s", memory_limit)
        conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    conn.execute("DROP FUNCTION IF EXISTS ua_signature")
    conn.create_function("ua_signature", ua_signature)

    decompress_dir = decompressed_dir

    train_tables: list[str] = []
    click_tables: list[str] = []
    for train_dir in train_dirs:
        round_name = train_dir.name
        imp_files_raw = collect_files(train_dir, "imp*.txt*")
        clk_files_raw = collect_files(train_dir, "clk*.txt*")
        imp_files = ensure_uncompressed(
            imp_files_raw,
            dataset_root=dataset_root,
            cache_dir=decompress_dir,
            workers=decompress_workers,
            force=force_decompress,
            verbose=verbose,
        )
        clk_files = ensure_uncompressed(
            clk_files_raw,
            dataset_root=dataset_root,
            cache_dir=decompress_dir,
            workers=decompress_workers,
            force=force_decompress,
            verbose=verbose,
        )
        progress.step(f"{round_name} 압축 해제 및 로드 준비 (학습)")
        imp_table = f"{round_name}_impressions"
        clk_table = f"{round_name}_clicks"
        register_impressions(conn, imp_table, imp_files, base_schema, round_name)
        register_clicks(conn, clk_table, clk_files, base_schema, round_name)
        train_tables.append(imp_table)
        click_tables.append(clk_table)

    conn.execute(
        "CREATE OR REPLACE TABLE train_impressions AS "
        + " UNION ALL ".join(f"SELECT * FROM {table}" for table in train_tables)
    )
    conn.execute(
        "CREATE OR REPLACE TABLE train_clicks AS "
        + " UNION ALL ".join(f"SELECT * FROM {table}" for table in click_tables)
    )

    test_tables: list[str] = []
    for test_dir in test_dirs:
        round_name = test_dir.name
        imp_files_raw = collect_files(test_dir, "*.txt*")
        imp_files = ensure_uncompressed(
            imp_files_raw,
            dataset_root=dataset_root,
            cache_dir=decompress_dir,
            workers=decompress_workers,
            force=force_decompress,
            verbose=verbose,
        )
        progress.step(f"{round_name} 압축 해제 및 로드 준비 (테스트)")
        table = f"{round_name}_test"
        register_impressions(
            conn, table, imp_files, base_schema + TEST_ADDITIONAL_COLUMNS, round_name
        )
        test_tables.append(table)

    conn.execute(
        "CREATE OR REPLACE TABLE test_impressions AS "
        + " UNION ALL ".join(f"SELECT * FROM {table}" for table in test_tables)
    )

    progress.step("DuckDB 로드 완료: 학습 인상 로그")
    progress.step("DuckDB 로드 완료: 학습 클릭 로그")
    progress.step("DuckDB 로드 완료: 테스트 로그")
    build_processed_table(
        conn,
        table_name="train_processed",
        impressions_table="train_impressions",
        columns=base_schema,
        click_table="train_clicks",
        nclick_column=None,
    )
    progress.step("파생 컬럼 계산 완료: 학습 데이터")
    build_processed_table(
        conn,
        table_name="test_processed",
        impressions_table="test_impressions",
        columns=base_schema + TEST_ADDITIONAL_COLUMNS,
        click_table=None,
        nclick_column="nclick",
    )
    progress.step("파생 컬럼 계산 완료: 테스트 데이터")

    export_table(conn, "train_processed", train_output)
    progress.step("TSV 내보내기 완료: 학습 데이터")
    export_table(conn, "test_processed", test_output)
    progress.step("TSV 내보내기 완료: 테스트 데이터")

    if split_advertisers:
        export_advertisers(
            conn,
            "train_processed",
            advertiser_column,
            output_dir,
            "train.tsv",
        )
        progress.step("광고주 분리 완료: 학습 데이터")
        export_advertisers(
            conn,
            "test_processed",
            advertiser_column,
            output_dir,
            "test.tsv",
        )
        progress.step("광고주 분리 완료: 테스트 데이터")

    progress.finish()

    logger.success("학습 TSV 생성: {}", train_output)
    logger.success("테스트 TSV 생성: {}", test_output)
    duration = format_duration(time.time() - pipeline_start)
    logger.success("파이프라인 완료 ({} 단계, {} 소요)", total_steps, duration)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("파이프라인 실패: {}", exc)
        sys.exit(1)
