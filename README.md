# Springer Capital — Data Engineer Intern Final Assessment

This repository implements the supplied referral-program Final Assessment with **Python + Pandas** for the transformation pipeline.

The pipeline loads the seven supplied CSV files, profiles every source table, cleans and de-duplicates the data, converts UTC timestamps to local time, joins the tables according to the ERD, derives `referral_source_category`, applies the required referral-reward/fraud rules, and writes the required **46-row** final CSV report.

This solution intentionally uses **Python + Pandas only**. PySpark/SparkSQL are not used by the pipeline, so they are not installed in the Docker image.

## Project structure

```text
.
├── your_script.py
├── Dockerfile
├── requirements.txt
├── README.md
├── .dockerignore
├── data/
│   ├── lead_log.csv
│   ├── paid_transactions.csv
│   ├── referral_rewards.csv
│   ├── user_logs.csv
│   ├── user_referral_logs.csv
│   ├── user_referral_statuses.csv
│   └── user_referrals.csv
├── docs/
│   └── data_dictionary.xlsx
└── output/
    ├── profiling_results.csv
    └── report.csv
```

## Important ERD rule: `referee_id`

The ERD note for `user_referrals.referee_id` says:

```text
Only join to leads if referral_source is Lead,
otherwise use referee_name and referee_phone.
```

The code therefore **does not join every `referee_id` to `lead_log.lead_id`**.

It creates a conditional lead-join key:

```python
lead_source_mask = df["referral_source"].eq("Lead")
df["lead_join_id"] = df["referee_id"].where(lead_source_mask, pd.NA)
```

Only rows whose `referral_source` is `Lead` can match `lead_log.lead_id`. For `User Sign Up` and `Draft Transaction`, the pipeline keeps the `referee_name` and `referee_phone` supplied directly by `user_referrals` and does not use `referee_id` to fetch a lead record.

The original `referee_id` value is still preserved in the final report because the PDF requires that output column; it is simply not used as a lead join key for non-Lead referrals.

## Requirements implemented

### 1. Data profiling

`output/profiling_results.csv` contains one row per source column with:

- data type
- row count
- null count
- null percentage
- distinct value count
- minimum value
- maximum value

The PDF requires at least null count and distinct-value count; the report includes those plus extra basic metrics.

### 2. Data loading and cleaning

All seven CSV files are loaded into Pandas DataFrames. The pipeline:

- trims surrounding whitespace from string columns
- normalizes important ID/data types
- normalizes booleans
- parses timestamps as UTC
- reduces repeated log records before joins so a referral is not duplicated

### 3. Table joins and de-duplication

`user_referrals` is the base table, preserving all **46 referrals**.

- `user_referral_statuses`: matched by `user_referral_status_id`
- `referral_rewards`: matched by `referral_reward_id`
- `user_logs`: latest available user-log record matched by `referrer_id`
- `lead_log`: matched by `referee_id` **only when `referral_source == "Lead"`**
- `user_referral_logs`: aggregated to one row per referral before joining
- `paid_transactions`: reduced to one row per transaction ID before joining

For repeated logs:

- `user_logs`: highest log `id` per `user_id` is retained because the supplied schema contains no user-log timestamp
- `lead_log`: latest `created_at`, then highest `id`, per `lead_id`
- `user_referral_logs`:
  - latest log ID -> `referral_details_id`
  - most recent non-null `source_transaction_id`
  - reward granted -> true if any log says true
  - most recent granted-log timestamp -> `reward_granted_at`

### 4. Time adjustment

All source timestamp values used by the pipeline are first parsed as UTC.

For `user_referrals`, which has no timezone column, the referral/update timezone is resolved from related data in this order:

1. referrer's `timezone_homeclub`
2. Lead's `timezone_location` (**available only for Lead referrals because of the conditional ERD join**)
3. transaction's `timezone_transaction`
4. UTC fallback when no related timezone can be resolved

`transaction_at` is displayed using `timezone_transaction` when available.

Chronological fraud checks compare the original UTC instants so differing timezones cannot reverse the real event order. The same-month check converts the transaction to the resolved referral timezone before comparing calendar months.

### 5. `referral_source_category`

The exact requested mapping is implemented:

```text
User Sign Up      -> Online
Draft Transaction -> Offline
Lead              -> lead_log.source_category
```

Because `lead_log` is conditionally joined, a Lead category can only come from the lead related to a `Lead` referral.

### 6. Business logic / fraud detection

`is_business_logic_valid` is Boolean.

**Valid condition 1** requires all ten PDF conditions:

1. reward value > 0
2. status = `Berhasil`
3. transaction ID exists
4. transaction status = `PAID`
5. transaction type = `NEW`
6. transaction occurs after the referral
7. transaction is in the same month as the referral
8. referrer membership has not expired
9. referrer account is not deleted
10. reward has been granted

**Valid condition 2** requires:

- status = `Menunggu` or `Tidak Berhasil`
- no reward value assigned

The five explicitly listed invalid conditions are also implemented. The five explicitly listed invalid conditions are also implemented. If an invalid condition is satisfied, the row is classified as invalid.

### 7. String adjustment

Initcap/title case is applied to human-readable text fields. Identifiers and anonymized/hash-like values are not case-modified because changing their case could corrupt identifiers. Club/location names keep their source casing.

### 8. Null handling

The final in-memory report contains no Pandas `NA`/`NaN` values.

Optional missing relationships are written as blank CSV fields instead of inventing fake IDs, dates, users, leads, or transactions. `referral_details_id` and `num_reward_days` use `0` when no corresponding value exists.

### 9. Required output

`output/report.csv` contains:

- **46 rows**
- **22 required columns**
- no duplicate `referral_id`
- Boolean `is_business_logic_valid`

Current verified result on the supplied data:

```text
Valid rows:   24
Invalid rows: 22
```

## Run locally

From the project root:

```bash
python -m pip install -r requirements.txt
python your_script.py --input-dir ./data --output-dir ./output
```

Generated files:

```text
output/report.csv
output/profiling_results.csv
```

## Run with Docker

Build the image:

```bash
docker build -t springer-referral-pipeline .
```

### macOS / Linux

Mount the host `output` directory so generated reports are stored **outside the container**:

```bash
mkdir -p output
docker run --rm \
  -v "$(pwd)/output:/app/output" \
  springer-referral-pipeline
```

### Windows PowerShell

```powershell
New-Item -ItemType Directory -Force output
docker run --rm `
  -v "${PWD}/output:/app/output" `
  springer-referral-pipeline
```

After execution, the host machine contains:

```text
output/report.csv
output/profiling_results.csv
```

## Output schema

The final report contains the 22 columns requested in the PDF, in the requested order:

```text
referral_details_id
referral_id
referral_source
referral_source_category
referral_at
referrer_id
referrer_name
referrer_phone_number
referrer_homeclub
referee_id
referee_name
referee_phone
referral_status
num_reward_days
transaction_id
transaction_status
transaction_at
transaction_location
transaction_type
updated_at
reward_granted_at
is_business_logic_valid
```

Business-friendly definitions are in `docs/data_dictionary.xlsx`.


