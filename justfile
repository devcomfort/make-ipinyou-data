# Justfile for iPinYou Data Pipeline
# .env 파일을 자동으로 로드합니다.
set dotenv-load := true
set shell := ["bash", "-c"]

# --- Variables from .env (with defaults) ---
python := "python3"
base := "."

# .env에서 가져오거나 기본값 사용
dataset_root := env_var_or_default("IPINYOU_DATASET_ROOT", "./original-data/ipinyou.contest.dataset")
output_dir := env_var_or_default("IPINYOU_OUTPUT_DIR", "./output_data")
schema_file := env_var_or_default("IPINYOU_SCHEMA_FILE", "schema.txt")

# Legacy workflow specific paths (Hardcoded to match legacy python scripts)
legacy_train_dir := dataset_root / "train"
legacy_test_dir := dataset_root / "test"
legacy_all_dir := base / "all"

# --- Main Commands ---

# Default: Run the modern DuckDB Pipeline using .env settings
default:
    @echo "🚀 Running DuckDB Pipeline..."
    @echo "   - Input: {{dataset_root}}"
    @echo "   - Output: {{output_dir}}"
    {{python}} {{base}}/duckdb_pipeline.py

duckdb:
    {{python}} {{base}}/duckdb_pipeline.py

# Check configuration
config:
    @echo "Current Configuration:"
    @echo "  DATASET_ROOT: {{dataset_root}}"
    @echo "  OUTPUT_DIR:   {{output_dir}}"
    @echo "  SCHEMA_FILE:  {{schema_file}}"
    @echo "  MEMORY_LIMIT: $IPINYOU_DUCKDB_MEMORY_LIMIT"
    @echo "  THREADS:      $IPINYOU_DUCKDB_THREADS"

# Clean output directories
clean:
    @echo "Cleaning output directories..."
    rm -rf {{output_dir}}
    rm -rf {{legacy_all_dir}}
    # Legacy specific cleanups
    rm -rf {{base}}/1458 {{base}}/2259 {{base}}/2261 {{base}}/2821 {{base}}/2997 
    rm -rf {{base}}/3358 {{base}}/3386 {{base}}/3427 {{base}}/3476

# --- Legacy Workflow Commands ---
# 기존 Makefile 로직을 유지하되, 입력 경로는 .env의 IPINYOU_DATASET_ROOT를 참조합니다.

legacy-all: legacy-init legacy-clk legacy-train-log legacy-test-log legacy-advertisers legacy-yzx

# 1. Initialize & Decompress (Legacy style inside source folder)
legacy-init:
    @echo "Legacy Init: Preparing data in {{dataset_root}}..."
    # Note: Legacy scripts expect these folders to be writeable
    mkdir -p {{dataset_root}}/train {{dataset_root}}/test
    
    @echo "Copying/Decompressing Training Data..."
    cp {{dataset_root}}/training2nd/imp.*.bz2 {{dataset_root}}/train/
    cp {{dataset_root}}/training2nd/clk.*.bz2 {{dataset_root}}/train/
    cp {{dataset_root}}/training3rd/imp.*.bz2 {{dataset_root}}/train/
    cp {{dataset_root}}/training3rd/clk.*.bz2 {{dataset_root}}/train/
    bzip2 -d -f {{dataset_root}}/train/*

    @echo "Copying/Decompressing Testing Data..."
    cp {{dataset_root}}/testing2nd/* {{dataset_root}}/test/
    cp {{dataset_root}}/testing3rd/* {{dataset_root}}/test/
    bzip2 -d -f {{dataset_root}}/test/*
    
    mkdir -p {{legacy_all_dir}}

# 2. Consolidate Click Logs
legacy-clk:
    cat {{dataset_root}}/train/clk*.txt > {{legacy_all_dir}}/clk.all.txt

# 3. Process Training Logs
legacy-train-log:
    cat {{dataset_root}}/train/imp*.txt | {{python}} {{base}}/python/mkdata.py {{schema_file}} {{legacy_all_dir}}/clk.all.txt > {{legacy_all_dir}}/train.log.txt
    {{python}} {{base}}/python/formalizeua.py {{legacy_all_dir}}/train.log.txt

# 4. Process Test Logs
legacy-test-log:
    cat {{dataset_root}}/test/*.txt | {{python}} {{base}}/python/mktest.py {{schema_file}} > {{legacy_all_dir}}/test.log.txt
    {{python}} {{base}}/python/formalizeua.py {{legacy_all_dir}}/test.log.txt

# 5. Split by Advertisers
legacy-advertisers:
    {{python}} {{base}}/python/splitadvertisers.py {{base}} 25 {{legacy_all_dir}}/train.log.txt {{legacy_all_dir}}/test.log.txt

# 6. Generate YZX format
legacy-yzx:
    bash {{base}}/mkyzxdata.sh