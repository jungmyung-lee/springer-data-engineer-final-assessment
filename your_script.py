"""Springer Capital - Data Engineer Intern Take-Home Test.

Pandas-only data pipeline for the referral program dataset.

The script:
1. Loads all seven CSV source tables.
2. Profiles every source column (null count, distinct count, etc.).
3. Cleans and de-duplicates log/reference tables.
4. Converts UTC timestamps to local time using source time zones.
5. Joins the seven source tables while preserving one row per referral,
   including the ERD rule that referee_id joins lead_log only for Lead rows.
6. Derives referral_source_category.
7. Applies the referral reward business rules / fraud checks.
8. Writes the required 46-row CSV report.

Usage:
    python your_script.py --input-dir ./data --output-dir ./output
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# The first name in each list is the filename supplied with the take-home test.
# lead_logs.csv is also accepted because that is the table name used in the PDF.
INPUT_FILE_CANDIDATES = {
    "lead_log": ["lead_log.csv", "lead_logs.csv"],
    "paid_transactions": ["paid_transactions.csv"],
    "referral_rewards": ["referral_rewards.csv"],
    "user_logs": ["user_logs.csv"],
    "user_referral_logs": ["user_referral_logs.csv"],
    "user_referral_statuses": ["user_referral_statuses.csv"],
    "user_referrals": ["user_referrals.csv"],
}

FINAL_COLUMNS = [
    "referral_details_id",
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "num_reward_days",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "reward_granted_at",
    "is_business_logic_valid",
]


# -----------------------------------------------------------------------------
# Input / profiling helpers
# -----------------------------------------------------------------------------

def find_input_file(input_dir: Path, candidate_names: Iterable[str]) -> Path:
    """Return the first existing candidate file from input_dir."""
    for name in candidate_names:
        path = input_dir / name
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Could not find any of {list(candidate_names)} in {input_dir.resolve()}"
    )


def load_tables(input_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load all seven CSV source tables into Pandas DataFrames."""
    tables: Dict[str, pd.DataFrame] = {}
    for table_name, candidate_names in INPUT_FILE_CANDIDATES.items():
        csv_path = find_input_file(input_dir, candidate_names)
        tables[table_name] = pd.read_csv(csv_path)
    return tables


def safe_min_max(series: pd.Series) -> tuple[str, str]:
    """Return printable min/max values without failing on mixed data types."""
    values = series.dropna()
    if values.empty:
        return "", ""

    try:
        minimum = values.min()
        maximum = values.max()
    except (TypeError, ValueError):
        # Mixed types are compared as strings only for profiling display.
        as_text = values.astype(str)
        minimum = as_text.min()
        maximum = as_text.max()

    return str(minimum), str(maximum)


def profile_table(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Profile one source table at the column level.

    The PDF requires at least null count and distinct value count. Extra basic
    metrics are included to make the profiling output more useful.
    """
    rows = []
    for column in df.columns:
        series = df[column]
        minimum, maximum = safe_min_max(series)
        rows.append(
            {
                "table_name": table_name,
                "column_name": column,
                "data_type": str(series.dtype),
                "row_count": int(len(df)),
                "null_count": int(series.isna().sum()),
                "null_percentage": round(float(series.isna().mean() * 100), 2),
                "distinct_value_count": int(series.nunique(dropna=True)),
                "minimum_value": minimum,
                "maximum_value": maximum,
            }
        )
    return pd.DataFrame(rows)


def create_profiling_report(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create one profiling report for every column in every source table."""
    profiles = [profile_table(name, df) for name, df in tables.items()]
    return pd.concat(profiles, ignore_index=True)


# -----------------------------------------------------------------------------
# Cleaning / date helpers
# -----------------------------------------------------------------------------

def parse_utc(series: pd.Series) -> pd.Series:
    """Parse a timestamp series as timezone-aware UTC."""
    return pd.to_datetime(series, utc=True, errors="coerce")


def normalize_boolean(series: pd.Series) -> pd.Series:
    """Convert common boolean encodings to True/False."""
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)

    normalized = series.astype("string").str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y", "t"})


def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim surrounding whitespace on string/object columns."""
    cleaned = df.copy()
    for column in cleaned.columns:
        if pd.api.types.is_object_dtype(cleaned[column]) or isinstance(
            cleaned[column].dtype, pd.StringDtype
        ):
            cleaned[column] = cleaned[column].astype("string").str.strip()
    return cleaned


def convert_utc_to_local_naive(
    utc_values: pd.Series, timezone_values: pd.Series
) -> pd.Series:
    """Convert UTC instants into local wall-clock time.

    Different rows can use different time zones. The returned values are
    timezone-naive because the PDF's sample output uses `YYYY-MM-DD HH:MM:SS`
    without an offset.

    If a timezone cannot be resolved after joins, UTC is used as a safe fallback
    instead of fabricating a location.
    """
    parsed = pd.to_datetime(utc_values, utc=True, errors="coerce")
    timezones = (
        pd.Series(timezone_values, index=parsed.index, dtype="object")
        .fillna("UTC")
        .replace("", "UTC")
    )

    result = pd.Series(pd.NaT, index=parsed.index, dtype="datetime64[ns]")

    for timezone_name in timezones.unique():
        mask = timezones.eq(timezone_name)
        try:
            converted = (
                parsed.loc[mask]
                .dt.tz_convert(timezone_name)
                .dt.tz_localize(None)
            )
        except (TypeError, ValueError, KeyError):
            # Invalid/unavailable timezone string -> retain the original UTC
            # instant and remove the timezone marker for output consistency.
            converted = (
                parsed.loc[mask]
                .dt.tz_convert("UTC")
                .dt.tz_localize(None)
            )
        result.loc[mask] = converted

    return result


def extract_reward_days(series: pd.Series) -> pd.Series:
    """Convert reward strings such as '10 days' into numeric day counts."""
    extracted = series.astype("string").str.extract(
        r"(\d+(?:\.\d+)?)", expand=False
    )
    return pd.to_numeric(extracted, errors="coerce")


def is_anonymized_hex(value: object) -> bool:
    """Detect anonymized/hash-like values so initcap does not corrupt IDs."""
    if pd.isna(value):
        return False
    return bool(re.fullmatch(r"[0-9a-fA-F]{24,64}", str(value).strip()))


def initcap_text(value: object) -> object:
    """Apply SQL-like initcap to readable text while preserving hash strings."""
    if pd.isna(value):
        return value
    text = str(value)
    if is_anonymized_hex(text):
        return text
    return text.title()


# -----------------------------------------------------------------------------
# Log de-duplication helpers
# -----------------------------------------------------------------------------

def latest_user_logs(user_logs: pd.DataFrame) -> pd.DataFrame:
    """Keep one current row per user.

    user_logs has a log ID but no event timestamp in the supplied schema. The
    highest log ID is therefore used deterministically as the latest record.
    """
    return (
        user_logs.sort_values("id")
        .drop_duplicates("user_id", keep="last")
        .copy()
    )


def latest_lead_logs(lead_logs: pd.DataFrame) -> pd.DataFrame:
    """Keep one latest row per lead based on created_at, then id."""
    result = lead_logs.copy()
    result["_created_at_utc"] = parse_utc(result["created_at"])
    result = (
        result.sort_values(["lead_id", "_created_at_utc", "id"])
        .drop_duplicates("lead_id", keep="last")
        .drop(columns="_created_at_utc")
    )
    return result


def aggregate_user_referral_logs(
    user_referral_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse one-to-many referral logs to one row per referral.

    - referral_details_id: ID from the chronologically latest log row.
    - source_transaction_id: most recent non-null source transaction.
    - is_reward_granted: True if any log says the reward was granted.
    - reward_granted_at_utc: most recent timestamp of a granted log.
    """
    logs = user_referral_logs.copy()
    logs["is_reward_granted"] = normalize_boolean(logs["is_reward_granted"])
    logs["_created_at_utc"] = parse_utc(logs["created_at"])

    latest = (
        logs.sort_values(["user_referral_id", "_created_at_utc", "id"])
        .drop_duplicates("user_referral_id", keep="last")
        [["user_referral_id", "id"]]
        .rename(columns={"id": "referral_details_id"})
    )

    non_null_transactions = logs[
        logs["source_transaction_id"].notna()
        & logs["source_transaction_id"].astype(str).str.strip().ne("")
    ]
    latest_transaction = (
        non_null_transactions.sort_values(
            ["user_referral_id", "_created_at_utc", "id"]
        )
        .drop_duplicates("user_referral_id", keep="last")
        [["user_referral_id", "source_transaction_id"]]
    )

    granted_logs = logs[logs["is_reward_granted"]].copy()
    granted = (
        granted_logs.groupby("user_referral_id", as_index=False)
        .agg(reward_granted_at_utc=("_created_at_utc", "max"))
    )
    granted["is_reward_granted"] = True

    result = latest.merge(
        latest_transaction, on="user_referral_id", how="left"
    ).merge(granted, on="user_referral_id", how="left")

    result["is_reward_granted"] = result["is_reward_granted"].eq(True)
    return result


# -----------------------------------------------------------------------------
# Main transformation
# -----------------------------------------------------------------------------

def build_report(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build the exact final referral report requested by the PDF."""

    # Work on copies so profiling remains a view of the original input.
    referrals = strip_string_columns(tables["user_referrals"])
    statuses = strip_string_columns(tables["user_referral_statuses"])
    rewards = strip_string_columns(tables["referral_rewards"])
    transactions = strip_string_columns(tables["paid_transactions"])
    users = strip_string_columns(tables["user_logs"])
    leads = strip_string_columns(tables["lead_log"])
    referral_logs = strip_string_columns(tables["user_referral_logs"])

    # Correct important data types before joining.
    referrals["user_referral_status_id"] = pd.to_numeric(
        referrals["user_referral_status_id"], errors="coerce"
    ).astype("Int64")
    referrals["referral_reward_id"] = pd.to_numeric(
        referrals["referral_reward_id"], errors="coerce"
    ).astype("Int64")
    statuses["id"] = pd.to_numeric(statuses["id"], errors="coerce").astype(
        "Int64"
    )
    rewards["id"] = pd.to_numeric(rewards["id"], errors="coerce").astype(
        "Int64"
    )
    users["is_deleted"] = normalize_boolean(users["is_deleted"])
    referral_logs["is_reward_granted"] = normalize_boolean(
        referral_logs["is_reward_granted"]
    )

    # Parse all UTC timestamps used in the transformation/business logic.
    referrals["referral_at_utc"] = parse_utc(referrals["referral_at"])
    referrals["updated_at_utc"] = parse_utc(referrals["updated_at"])
    transactions["transaction_at_utc"] = parse_utc(
        transactions["transaction_at"]
    )

    # ------------------------------------------------------------------
    # De-duplicate source logs before joining so one referral stays one row.
    # ------------------------------------------------------------------
    users = latest_user_logs(users)
    leads = latest_lead_logs(leads)
    referral_log_details = aggregate_user_referral_logs(referral_logs)

    # Dimension/status lookup.
    statuses = (
        statuses.sort_values("id")
        .drop_duplicates("id", keep="last")
        [["id", "description"]]
        .rename(
            columns={
                "id": "user_referral_status_id",
                "description": "referral_status",
            }
        )
    )

    # Reward lookup. `10 days` -> 10.
    rewards["num_reward_days"] = extract_reward_days(rewards["reward_value"])
    rewards = (
        rewards.sort_values("id")
        .drop_duplicates("id", keep="last")
        [["id", "num_reward_days"]]
        .rename(columns={"id": "referral_reward_id"})
    )

    # Referrer user lookup.
    users = users.rename(
        columns={
            "user_id": "referrer_id",
            "name": "referrer_name",
            "phone_number": "referrer_phone_number",
            "homeclub": "referrer_homeclub",
        }
    )[
        [
            "referrer_id",
            "referrer_name",
            "referrer_phone_number",
            "referrer_homeclub",
            "timezone_homeclub",
            "membership_expired_date",
            "is_deleted",
        ]
    ]

    # Lead lookup used when referral_source == Lead.
    leads = leads[
        ["lead_id", "source_category", "timezone_location"]
    ].rename(
        columns={
            "lead_id": "lead_referee_id",
            "source_category": "lead_source_category",
        }
    )

    referral_log_details = referral_log_details.rename(
        columns={"user_referral_id": "referral_id"}
    )

    # Base table is user_referrals. Left joins intentionally preserve all
    # 46 source referrals even when optional related data is missing.
    df = referrals.merge(
        statuses,
        on="user_referral_status_id",
        how="left",
        validate="many_to_one",
    )
    df = df.merge(
        rewards,
        on="referral_reward_id",
        how="left",
        validate="many_to_one",
    )
    df = df.merge(
        users, on="referrer_id", how="left", validate="many_to_one"
    )

    # ERD note: referee_id is used to join lead_logs ONLY when
    # referral_source == "Lead". For User Sign Up / Draft Transaction rows,
    # referee_name and referee_phone already come directly from user_referrals.
    # Building a conditional join key prevents a non-Lead referee_id from ever
    # being interpreted as a lead_id, even if the raw ID happens to match one.
    lead_source_mask = df["referral_source"].eq("Lead")
    df["lead_join_id"] = df["referee_id"].where(lead_source_mask, pd.NA)
    df = df.merge(
        leads,
        left_on="lead_join_id",
        right_on="lead_referee_id",
        how="left",
        validate="many_to_one",
    )

    # Defensive validation of the ERD relationship rule.
    non_lead_mask = ~df["referral_source"].eq("Lead")
    if df.loc[non_lead_mask, "lead_referee_id"].notna().any():
        raise ValueError(
            "Non-Lead referrals must not join to lead_logs via referee_id."
        )

    df = df.merge(
        referral_log_details,
        on="referral_id",
        how="left",
        validate="one_to_one",
    )

    # user_referrals.transaction_id is primary. If it is absent, the referral
    # detail log's source_transaction_id is used as a fallback.
    main_transaction = df["transaction_id"].astype("string")
    fallback_transaction = df["source_transaction_id"].astype("string")
    df["effective_transaction_id"] = main_transaction.fillna(
        fallback_transaction
    )
    blank_transaction = (
        df["effective_transaction_id"].fillna("").str.strip().eq("")
    )
    df.loc[blank_transaction, "effective_transaction_id"] = pd.NA

    # Ensure one transaction record per transaction ID before the join.
    transactions = (
        transactions.sort_values("transaction_at_utc")
        .drop_duplicates("transaction_id", keep="last")
        .rename(columns={"transaction_id": "effective_transaction_id"})
    )
    df = df.merge(
        transactions[
            [
                "effective_transaction_id",
                "transaction_status",
                "transaction_at_utc",
                "transaction_location",
                "timezone_transaction",
                "transaction_type",
            ]
        ],
        on="effective_transaction_id",
        how="left",
        validate="many_to_one",
    )

    # ------------------------------------------------------------------
    # Time adjustment
    # ------------------------------------------------------------------
    # user_referrals itself has no timezone column. We resolve one from related
    # data in the following order:
    #   referrer's home club -> Lead location (Lead rows only)
    #   -> transaction location -> UTC.
    df["referral_timezone"] = (
        df["timezone_homeclub"]
        .combine_first(df["timezone_location"])
        .combine_first(df["timezone_transaction"])
        .fillna("UTC")
    )

    df["referral_at_local"] = convert_utc_to_local_naive(
        df["referral_at_utc"], df["referral_timezone"]
    )
    df["updated_at_local"] = convert_utc_to_local_naive(
        df["updated_at_utc"], df["referral_timezone"]
    )
    df["transaction_at_local"] = convert_utc_to_local_naive(
        df["transaction_at_utc"],
        df["timezone_transaction"].combine_first(df["referral_timezone"]),
    )
    df["reward_granted_at_local"] = convert_utc_to_local_naive(
        df["reward_granted_at_utc"], df["referral_timezone"]
    )

    # For the "same month" business rule, both timestamps must be viewed in
    # the same local timezone. Otherwise two different local timezones could
    # create a false month/ordering result.
    df["transaction_at_referral_tz"] = convert_utc_to_local_naive(
        df["transaction_at_utc"], df["referral_timezone"]
    )

    # ------------------------------------------------------------------
    # referral_source_category exactly as defined in the PDF.
    # ------------------------------------------------------------------
    df["referral_source_category"] = pd.Series(
        pd.NA, index=df.index, dtype="object"
    )
    df.loc[
        df["referral_source"].eq("User Sign Up"),
        "referral_source_category",
    ] = "Online"
    df.loc[
        df["referral_source"].eq("Draft Transaction"),
        "referral_source_category",
    ] = "Offline"
    df.loc[lead_source_mask, "referral_source_category"] = df.loc[
        lead_source_mask, "lead_source_category"
    ]

    # ------------------------------------------------------------------
    # Business logic / potential fraud detection
    # ------------------------------------------------------------------
    reward_days = pd.to_numeric(df["num_reward_days"], errors="coerce").fillna(
        0
    )
    has_reward = reward_days.gt(0)
    no_reward = ~has_reward

    has_transaction = df["effective_transaction_id"].notna() & (
        df["effective_transaction_id"].fillna("").astype(str).str.strip().ne("")
    )
    transaction_paid = (
        df["transaction_status"].fillna("").astype(str).str.upper().eq("PAID")
    )
    transaction_new = (
        df["transaction_type"].fillna("").astype(str).str.upper().eq("NEW")
    )

    # Chronological comparison is done on the original UTC instants. This is
    # necessary because comparing naive local clocks from different zones can
    # reverse the apparent order even when the actual instant is later.
    transaction_after_referral = (
        df["transaction_at_utc"].notna()
        & df["referral_at_utc"].notna()
        & df["transaction_at_utc"].gt(df["referral_at_utc"])
    )
    transaction_before_referral = (
        df["transaction_at_utc"].notna()
        & df["referral_at_utc"].notna()
        & df["transaction_at_utc"].lt(df["referral_at_utc"])
    )

    same_month = (
        df["transaction_at_referral_tz"].notna()
        & df["referral_at_local"].notna()
        & df["transaction_at_referral_tz"]
        .dt.to_period("M")
        .eq(df["referral_at_local"].dt.to_period("M"))
    )

    membership_expired_date = pd.to_datetime(
        df["membership_expired_date"], errors="coerce"
    ).dt.normalize()
    membership_reference_date = (
        df["transaction_at_referral_tz"]
        .combine_first(df["referral_at_local"])
        .dt.normalize()
    )
    membership_active = (
        membership_expired_date.notna()
        & membership_reference_date.notna()
        & membership_expired_date.ge(membership_reference_date)
    )

    referrer_account_active = df["is_deleted"].eq(False).fillna(False)
    reward_granted = df["is_reward_granted"].eq(True)
    referral_success = (
        df["referral_status"].fillna("").astype(str).eq("Berhasil")
    )
    referral_pending_or_failed = (
        df["referral_status"]
        .fillna("")
        .astype(str)
        .isin(["Menunggu", "Tidak Berhasil"])
    )

    # Valid Condition 1: all ten conditions from the PDF must be true.
    valid_condition_1 = (
        has_reward
        & referral_success
        & has_transaction
        & transaction_paid
        & transaction_new
        & transaction_after_referral
        & same_month
        & membership_active
        & referrer_account_active
        & reward_granted
    )

    # Valid Condition 2.
    valid_condition_2 = referral_pending_or_failed & no_reward

    # Five explicit invalid conditions from the PDF.
    invalid_condition_1 = has_reward & ~referral_success
    invalid_condition_2 = has_reward & ~has_transaction
    invalid_condition_3 = (
        no_reward
        & has_transaction
        & transaction_paid
        & transaction_after_referral
    )
    invalid_condition_4 = referral_success & no_reward
    invalid_condition_5 = transaction_before_referral

    any_explicit_invalid = (
        invalid_condition_1
        | invalid_condition_2
        | invalid_condition_3
        | invalid_condition_4
        | invalid_condition_5
    )

    # An explicit invalid rule wins if it conflicts with the broad valid
    # Condition 2. Rows that do not satisfy either valid condition are also
    # invalid, because Condition 1 intentionally requires all ten checks.
    df["is_business_logic_valid"] = (
        (valid_condition_1 | valid_condition_2) & ~any_explicit_invalid
    ).astype(bool)

    # ------------------------------------------------------------------
    # String adjustment (Initcap)
    # ------------------------------------------------------------------
    # Apply initcap to readable text. IDs and anonymized/hash-like fields are
    # preserved because changing their case would corrupt join identifiers.
    # Club/location names stay in source casing, matching the PDF examples.
    initcap_columns = [
        "referral_source",
        "referral_source_category",
        "referral_status",
        "transaction_status",
        "transaction_type",
        "referrer_name",
        "referee_name",
    ]
    for column in initcap_columns:
        df[column] = df[column].map(initcap_text)

    # ------------------------------------------------------------------
    # Final output schema
    # ------------------------------------------------------------------
    report = pd.DataFrame(
        {
            "referral_details_id": pd.to_numeric(
                df["referral_details_id"], errors="coerce"
            )
            .fillna(0)
            .astype("int64"),
            "referral_id": df["referral_id"],
            "referral_source": df["referral_source"],
            "referral_source_category": df["referral_source_category"],
            "referral_at": df["referral_at_local"],
            "referrer_id": df["referrer_id"],
            "referrer_name": df["referrer_name"],
            "referrer_phone_number": df["referrer_phone_number"],
            "referrer_homeclub": df["referrer_homeclub"],
            "referee_id": df["referee_id"],
            "referee_name": df["referee_name"],
            "referee_phone": df["referee_phone"],
            "referral_status": df["referral_status"],
            "num_reward_days": pd.to_numeric(
                df["num_reward_days"], errors="coerce"
            )
            .fillna(0)
            .astype("int64"),
            "transaction_id": df["effective_transaction_id"],
            "transaction_status": df["transaction_status"],
            "transaction_at": df["transaction_at_local"],
            "transaction_location": df["transaction_location"],
            "transaction_type": df["transaction_type"],
            "updated_at": df["updated_at_local"],
            "reward_granted_at": df["reward_granted_at_local"],
            "is_business_logic_valid": df[
                "is_business_logic_valid"
            ].astype(bool),
        }
    )

    # Format timestamps exactly like the PDF's sample values.
    datetime_columns = [
        "referral_at",
        "transaction_at",
        "updated_at",
        "reward_granted_at",
    ]
    for column in datetime_columns:
        report[column] = (
            pd.to_datetime(report[column], errors="coerce")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .fillna("")
        )

    # Optional relationships legitimately have missing values. We export those
    # as blank strings rather than inventing fake IDs/dates. Numeric outputs use
    # 0 where no detail/reward exists. The resulting DataFrame has no Pandas NA.
    non_text_columns = {
        "referral_details_id",
        "num_reward_days",
        "is_business_logic_valid",
        *datetime_columns,
    }
    for column in report.columns:
        if column not in non_text_columns:
            report[column] = report[column].astype("string").fillna("")

    # Keep exact requested column order and verify no duplicate referral rows.
    report = report[FINAL_COLUMNS]
    if report["referral_id"].duplicated().any():
        raise ValueError("Duplicate referral_id values remain in the final report.")

    return report


# -----------------------------------------------------------------------------
# Validation / CLI
# -----------------------------------------------------------------------------

def validate_report(report: pd.DataFrame, expected_rows: int = 46) -> None:
    """Fail fast if the generated report violates key PDF requirements."""
    if list(report.columns) != FINAL_COLUMNS:
        raise ValueError("Final report columns do not match the required schema.")

    if len(report) != expected_rows:
        raise ValueError(
            f"Expected {expected_rows} output rows, but generated {len(report)}."
        )

    if report["referral_id"].duplicated().any():
        raise ValueError("Final report contains duplicate referral IDs.")

    if report.isna().any().any():
        raise ValueError("Final report still contains Pandas null values.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Springer Capital referral data pipeline (Pandas)."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing the seven source CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where report/profiling CSVs are written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading source files from: {args.input_dir.resolve()}")
    tables = load_tables(args.input_dir)

    # Data profiling is intentionally performed on the raw source tables.
    profiling = create_profiling_report(tables)
    profiling_path = args.output_dir / "profiling_results.csv"
    profiling.to_csv(profiling_path, index=False)

    report = build_report(tables)
    validate_report(report, expected_rows=46)

    report_path = args.output_dir / "report.csv"
    report.to_csv(report_path, index=False)

    valid_count = int(report["is_business_logic_valid"].sum())
    invalid_count = int((~report["is_business_logic_valid"]).sum())

    print("Pipeline completed successfully.")
    print(f"Report rows: {len(report)}")
    print(f"Valid business-logic rows: {valid_count}")
    print(f"Invalid business-logic rows: {invalid_count}")
    print(f"Report: {report_path.resolve()}")
    print(f"Profiling: {profiling_path.resolve()}")


if __name__ == "__main__":
    main()
