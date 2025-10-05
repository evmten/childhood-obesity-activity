#!/usr/bin/env python3
"""
Healthcare Activity/Obesity ETL (Associate DE level)

- Reads raw WHO CSVs from ADLS Gen2 with metadata before/after the table
- Detects header by column names, detects end by "3-commas"/4-column rule
- Builds processed activity/obesity layers and an inner-joined curated layer
- Includes logging, basic validations, and clean parameterization
"""
import os
import argparse
import logging
from typing import Iterable, List, Tuple, Sequence

import pandas as pd
from adlfs import AzureBlobFileSystem
from pathlib import Path

# ------------------------- Logging & CLI -------------------------

LOG = logging.getLogger("health_etl")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build processed & curated datasets for Health BI.")
    p.add_argument("--account", required=True, help="ADLS Gen2 account name")
    p.add_argument("--container", required=True, help="ADLS Gen2 container name")
    p.add_argument("--sas", default=os.getenv("ADLS_SAS"), help="SAS token (or set env ADLS_SAS)")
    p.add_argument("--ages", nargs="+", type=int, default=[11, 13, 15], help="Ages to process")
    p.add_argument("--activity-prefix", default="Percentages of physically active children among")
    p.add_argument("--obesity-prefix",  default="Prevalence of overweight (including obesity) among")
    p.add_argument("--write-parquet", action="store_true", help="Also write Parquet outputs")
    p.add_argument("--dry-run", action="store_true", help="Run validations; skip writes")
    p.add_argument("--debug", action="store_true", help="Verbose logs")
    return p.parse_args()

def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s :: %(message)s",
    )

    if not debug:
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("adlfs").setLevel(logging.WARNING)
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# ------------------------- Path helpers -------------------------

def abfs_path(container: str, *parts: str) -> str:
    """abfs:// path for pandas read_csv/to_csv with storage_options."""
    return "abfs://" + "/".join([container.strip("/")] + [p.strip("/") for p in parts])

def noscheme_path(container: str, *parts: str) -> str:
    """No-scheme path for abfs.open(...)."""
    return "/".join([container.strip("/")] + [p.strip("/") for p in parts])

# ------------------------- Core logic -------------------------

def find_table_bounds(lines: Sequence[str], required_cols: Iterable[str]) -> Tuple[int, int]:
    """
    Return (header_idx, data_rows).
    - header_idx: 0-based index of header line containing all required_cols
    - data_rows: number of contiguous data lines after header that have exactly 3 commas (4 columns)
    """
    header_idx = None
    for i, line in enumerate(lines):
        if all(col in line for col in required_cols):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Header not found.")

    data_rows = 0
    for line in lines[header_idx + 1:]:
        if line.count(",") == 3:
            data_rows += 1
        else:
            break
    return header_idx, data_rows

def read_measure(
    fs: AzureBlobFileSystem,
    container: str,
    ages: List[int],
    prefix: str,
    required_cols: Tuple[str, ...] = ("COUNTRY", "SEX", "YEAR", "VALUE"),
    storage_options: dict | None = None,
) -> pd.DataFrame:
    """
    Read + stack one measure (activity OR obesity) across ages.
    """

    dfs: List[pd.DataFrame] = []

    for age in ages:
        raw_name = f"{prefix} {age}-year-olds.csv"
        ns_path = noscheme_path(container, "raw", raw_name)      # for abfs.open()
        scheme_path = abfs_path(container, "raw", raw_name)      # for pandas

        with fs.open(ns_path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        header_idx, data_rows = find_table_bounds(lines, required_cols)
        LOG.info(f"[{raw_name}] header_idx={header_idx}, data_rows={data_rows}")

        if data_rows == 0:
            LOG.warning(f"[{raw_name}] No data rows detected; skipping.")
            continue

        df = pd.read_csv(
            scheme_path,
            skiprows=header_idx,          # header row is first row seen
            nrows=data_rows,              # exact number of data rows
            storage_options=storage_options,
        )

        # Standardize types early and add AGE
        df["YEAR"] = pd.to_numeric(df["YEAR"], errors="raise").astype("int64")
        df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce")
        df["AGE"] = age

        dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=["COUNTRY", "SEX", "YEAR", "VALUE", "AGE"])

    return pd.concat(dfs, ignore_index=True)

def validate_keys(df: pd.DataFrame, keys: List[str], name: str) -> None:
    """Basic key hygiene: not empty, no nulls in keys, no duplicate key rows."""
    if df.empty:
        raise AssertionError(f"{name} is empty.")
    if df[keys].isnull().any().any():
        bad = df[df[keys].isnull().any(axis=1)]
        raise AssertionError(f"{name} has null key values. Sample:\n{bad.head(5)}")
    dup_count = df.duplicated(subset=keys, keep=False).sum()
    if dup_count:
        dup_keys = (
            df.groupby(keys).size()
              .loc[lambda s: s > 1]
              .sort_values(ascending=False)
              .head(10)
        )
        raise AssertionError(f"{name} has {dup_count} duplicate rows on keys. Top combos:\n{dup_keys}")

# ------------------------- Orchestration -------------------------

def main() -> int:
    args = parse_args()
    setup_logging(args.debug)

    if not args.sas:
        LOG.warning("No SAS token provided. Use --sas or set env ADLS_SAS. (In ADF, prefer Managed Identity.)")

    fs = AzureBlobFileSystem(account_name=args.account, sas_token=args.sas)
    keys = ["COUNTRY", "AGE", "SEX", "YEAR"]

    # Read & rename processed layers
    opts = {"account_name": args.account, "sas_token": args.sas}

    df_activity = read_measure(
        fs, args.container, args.ages, args.activity_prefix,
        storage_options=opts
    ).rename(columns={"VALUE": "ACTIVITY_VAL"})

    df_obesity = read_measure(
        fs, args.container, args.ages, args.obesity_prefix,
        storage_options=opts
    ).rename(columns={"VALUE": "OBESITY_VAL"})
        
    LOG.info(f"Row counts: activity={len(df_activity)}, obesity={len(df_obesity)}")

    # Validate key hygiene
    validate_keys(df_activity, keys, "activity")
    validate_keys(df_obesity,  keys, "obesity")

    # Inner join for apples-to-apples comparison
    df_merged = pd.merge(df_activity, df_obesity, on=keys, how="inner", validate="one_to_one")
    LOG.info(f"Merged rows={len(df_merged)} (inner on {keys})")

    if args.dry_run:
        LOG.info("--dry-run: validations complete; skipping writes.")
        return 0
    
    # --- Local snapshot for reproducibility ---
    local_dir = Path(__file__).resolve().parents[1] / "data" / "curated"
    local_dir.mkdir(parents=True, exist_ok=True)
    (df_merged
        .to_csv(local_dir / "df_merged.csv", index=False))
    LOG.info(f"Wrote local snapshot to {local_dir}/df_merged.csv")
    if args.write_parquet:
        df_merged.to_parquet(local_dir / "df_merged.parquet", index=False)

    # Write processed layers (renamed already)
    act_csv = abfs_path(args.container, "processed", "activity_merged.csv")
    obe_csv = abfs_path(args.container, "processed", "obesity_merged.csv")
    cur_csv = abfs_path(args.container, "curated",  "df_merged.csv")

    df_activity.to_csv(act_csv, index=False, storage_options=opts)
    df_obesity.to_csv (obe_csv, index=False, storage_options=opts)
    df_merged.to_csv  (cur_csv, index=False, storage_options=opts)

    LOG.info(f"Wrote CSV:\n  {act_csv}\n  {obe_csv}\n  {cur_csv}")

    if args.write_parquet:
        act_parq = abfs_path(args.container, "processed", "activity_merged.parquet")
        obe_parq = abfs_path(args.container, "processed", "obesity_merged.parquet")
        cur_parq = abfs_path(args.container, "curated",  "df_merged.parquet")
        df_activity.to_parquet(act_parq, index=False, storage_options=opts)
        df_obesity.to_parquet (obe_parq, index=False, storage_options=opts)
        df_merged.to_parquet  (cur_parq, index=False, storage_options=opts)
        LOG.info(f"Wrote Parquet:\n  {act_parq}\n  {obe_parq}\n  {cur_parq}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())


# r"""
# Healthcare Activity/Obesity ETL

# Inputs (CLI or env):
# - --account  : ADLS Gen2 account name (e.g., childactivityobesity)
# - --container: ADLS Gen2 container name (e.g., activity-obesity-data)
# - --sas      : SAS token (or set env ADLS_SAS)
# - --ages     : Ages to process (default: 11 13 15)
# - --activity-prefix / --obesity-prefix: Raw file name prefixes
# - --dry-run  : Run validations only; skip writes
# - --debug    : Verbose logs (DEBUG); otherwise INFO

# Process:
# 1) Read raw CSVs from /raw with metadata before/after table
# 2) Detect header + contiguous table rows, read only the table
# 3) Build processed activity/obesity, then inner-join on [COUNTRY, AGE, SEX, YEAR]
# 4) Write processed/ and curated/ outputs to ADLS

# Outputs:
# - processed/activity_merged.(csv|parquet)
# - processed/obesity_merged.(csv|parquet)
# - curated/df_merged.(csv|parquet)

# Example (PowerShell):
#   $env:ADLS_SAS = "?sv=..."; `
#   python .\health_etl.py --account childactivityobesity --container activity-obesity-data --ages 11 13 15 --dry-run

# Example (Bash):
#   export ADLS_SAS='?sv=...'
#   python health_etl.py --account childactivityobesity --container activity-obesity-data --ages 11 13 15
# """