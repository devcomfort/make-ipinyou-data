# make-ipinyou-data

This repository converts the **iPinYou RTB dataset** into a clean, research-ready format.

> **🚀 2025 Update:** A new high-performance **DuckDB pipeline** is now available alongside the legacy scripts. It offers broader data coverage, faster execution, and better resource management.

## 🌟 2025 Pipeline Refresh (DuckDB)

The new pipeline (`duckdb_pipeline.py`) introduces significant improvements:

-   **Broader Data Coverage:** Automatically crawls all `training*` and `testing*` rounds. It decompresses archives into a cache and exports unified TSV files with a `round` column, making historical campaigns (1st/2nd/3rd rounds) available by default.
-   **Advanced UA Normalization:** Powered by `ua-parser[regex]`, it parses user-agent strings into normalized OS/Browser families, exposing a `ua_signature(useragent)` UDF for downstream tasks.
-   **High Performance:** Configurable via `.env` (memory limits, threads, workers). Uses DuckDB's `strict_mode=FALSE` and `null_padding=TRUE` to handle schema variations across rounds effectively.

### Pipeline Comparison

| Feature      | 🐢 Legacy (`make all`)                       | 🦆 DuckDB Pipeline (Recommended)                       |
| :----------- | :------------------------------------------ | :---------------------------------------------------- |
| **Coverage** | Per-campaign run (manual selection).        | **All rounds** (`training*`/`testing*`) auto-crawled. |
| **Parsing**  | Keyword heuristics (basic lists).           | **`ua-parser`** (Normalized OS/Browser families).     |
| **Output**   | Spread across numbered directories (~14GB). | Unified `train.tsv` / `test.tsv` + optional splits.   |
| **Control**  | System defaults (no memory cap).            | Fine-grained tuning via **`.env`** (Memory, Threads). |
| **Workflow** | `make all` -> Manual analysis.              | `python pipeline.py` -> Ready-to-use TSVs.            |

The legacy helpers under `python/` (e.g., `mkdata.py`, `mktest.py`, `formalizeua.py`) have all been ported to **Python 3** so previous experiments remain reproducible without the Python 2 toolchain.

---

## 0. Prerequisites (Common)

Before running either pipeline, you must download the source data.

1.  Download `ipinyou.contest.dataset.zip` from [Kaggle](https://www.kaggle.com/datasets/lastsummer/ipinyou).
2.  Unzip the file. You should see a folder named `ipinyou.contest.dataset` containing the raw files.
    * *Note: Do not unzip the individual `.bz2` archives inside the subfolders.*

```text
ipinyou.contest.dataset/
├── training1st/
├── training2nd/
├── training3rd/
├── testing1st/
├── city.en.txt
└── ...
````

-----

## Option A: Using the DuckDB Pipeline (Recommended)

This method is faster and produces a unified dataset.

### 1\. Configure Environment

Create a `.env` file in the repository root. You can copy the structure below:

```ini
# .env Configuration Example

# Path to the unzipped dataset (absolute path recommended)
IPINYOU_DATASET_ROOT=/path/to/ipinyou.contest.dataset

# Where to save the processed .tsv files
IPINYOU_OUTPUT_DIR=./output_data

# Performance Tuning
IPINYOU_DUCKDB_MEMORY_LIMIT=16GB
IPINYOU_DUCKDB_THREADS=4
IPINYOU_DECOMPRESS_WORKERS=4

# Logging
IPINYOU_VERBOSE=true
IPINYOU_PROGRESS=true

# Legacy workflow (optional overrides)
IPINYOU_LEGACY_ORIGINAL_FOLDER=/path/to/ipinyou.contest.dataset
#IPINYOU_LEGACY_TRAIN_DIR=/path/to/custom/train
#IPINYOU_LEGACY_TEST_DIR=/path/to/custom/test
```

### 2\. Run the Pipeline

Execute the Python script:

```bash
python duckdb_pipeline.py
```

Check your `IPINYOU_OUTPUT_DIR` for the generated `train.tsv` and `test.tsv` files.

-----

## Option B: Using the Legacy Workflow

Use this if you need to replicate original research relying on the specific folder structure of the old scripts.

### 1\. Configure Paths

Ensure `.env` defines `IPINYOU_LEGACY_ORIGINAL_FOLDER` (default: `original-data/ipinyou.contest.dataset`). If you prefer the historical symlink approach, you can still create `original-data/ipinyou.contest.dataset` as a symbolic link to your dataset root, but it is now optional.

### 2\. Build Data

Install [just](https://github.com/casey/just) and run from the repository root:

```bash
just --dotenv all
```

*Expect approximately 14GB of artifacts.* The output will be organized by campaign ID (e.g., `1458`, `2259`):

```text
make-ipinyou-data/
├── 1458/
├── 2259/
├── all/
├── original-data/
└── ...
```

-----

## Data Format Guide

### Legacy Format Details (e.g., folder `1458`)

If you use the legacy format, each campaign folder contains:

  * **`train.log.txt` / `test.log.txt`**: Original string features.
      * *Col 1*: Click label.
      * *Col 14*: Clearing price.
  * **`featindex.txt`**: Maps categorical values to integer IDs.
      * Example: `8:115.45.195.* 29` (Column 8 value maps to ID 29).
  * **`train.yzx.txt` / `test.yzx.txt`**: Vectorized format (LIBLINEAR/SVM style).
      * Format: `y` (Label) `z` (Win Price) `x` (Features `index:1`).
      * Reference: [iPinYou Benchmarking (ArXiv)](http://arxiv.org/abs/1407.7073).

### DuckDB Format Details

The DuckDB pipeline outputs standard **TSV (Tab-Separated Values)** files with headers, including a `round` column to distinguish between data waves (1st, 2nd, 3rd).

-----

## Contact

For questions or issues, please open a GitHub issue or contact [Weinan Zhang](http://www0.cs.ucl.ac.uk/staff/w.zhang/).
