# Rearc Data Quest — Process Notes


## What the pipeline does

Two sources come in:

- The BLS productivity series (`pr.*` files) from `download.bls.gov`.
- US population by year from the DataUSA API.

They land in a Unity Catalog Volume, get modelled through Bronze → Silver → Gold
in a single Lakeflow declarative pipeline, and the three questions from the quest
are answered by SQL views sitting on top of Gold.

Everything lives in one catalog, `rearc_dev`, split by layer:

| Schema | Purpose |
| --- | --- |
| `dataquest_raw` | The `landing` Volume, raw files as pulled from source |
| `dataquest_bronze` | Raw files read into Delta, still all strings |
| `dataquest_silver` | Cleaned, typed, deduped, one table per source table |
| `dataquest_gold` | The star model (fact + dimension + reference) |
| `dataquest_serving` | Views that answer the questions |

## Layout

```
Rearc_data_process/
  explorations/
    00_setup.py          catalog, schemas, volume
    01_ingest.py         pull source files into the Volume
    05_gold_serving.py   the three answers as views (+ PySpark equivalents)
  transformations/
    02_bronze.py         raw files -> bronze tables
    03_silver.py         clean/type/dedupe
    04_gold.py           star model
```

`00`, `01` and `05` run as normal notebooks. `02`, `03`, `04` are the source for
the declarative pipeline.

## Ingestion

`01_ingest.py` scrapes the BLS directory listing, downloads every `pr.*` file,
and writes them under `/Volumes/rearc_dev/dataquest_raw/landing/bls/pr`. Two
things that aren't obvious up front:

- BLS returns a 403 without a `User-Agent` carrying a contact email, so every
  request sets one.
- I keep a small `_manifest.json` in the Volume with each file's `Last-Modified`
  header. On a re-run I do a `HEAD` first and skip anything that hasn't changed,
  and I remove files that have disappeared from the server. That makes the pull
  safe to run repeatedly without re-downloading the whole directory each time.

The population API is a single JSON document, saved as `population.json`.

The values that change between environments catalog, schema, volume, contact
email are notebook widgets so a job or a bundle can pass them in.

## Bronze

Bronze reads the landed files and does as little as possible. Everything stays a
string, no trimming, no casting. The point of Bronze is to have a faithful,
queryable copy of what actually arrived, so if Silver logic is wrong I can debug
against Bronze instead of re-pulling from source.

I give the `pr.data` and `pr.series` tables explicit schemas rather than
inferring them, so a surprise column from upstream fails loudly instead of
silently shifting everything. The seven small `pr.*` lookup files are loaded in a
loop since they're all the same tab-separated shape. Population is read as
multiline JSON and the `data` array exploded into rows.

Bronze carries non-null key expectations, but as **warnings** (`@dp.expect_all`),
not drops. I want bad rows visible in the pipeline metrics at this stage, not
thrown away dropping is Silver's job.

## Silver

Silver is where the data becomes trustworthy: trim whitespace, cast `year` to
int and `value` to double, and dedupe on the natural key `(series_id, year,
period)`. Here the key expectations are `@dp.expect_all_or_drop` a row with no
series id, year or period has no business downstream, so it goes.

The one design decision worth calling out: **Silver does not join anything.** It
produces one cleaned table per Bronze table `silver_pr_data`,
`silver_pr_series`, the three lookups I actually use (`measure`, `sector`,
`seasonal`), and `silver_population`. No `series_label`, no denormalisation.

I went back and forth on this. An earlier version built the readable series label
inside Silver, and the result was that Gold had nothing left to do but rename
columns Silver and Gold looked identical, which is a smell. Moving the joins
out of Silver keeps each layer's job honest: Silver cleans, Gold models.

Population is filtered to `Nation = 'United States'`. In practice the API only
returns US rows, so this is a guard rather than a real filter, but the question
asks specifically about US population and I'd rather the intent be explicit in
code than rely on the source never changing.

## Gold

Gold is the star model and it's where the actual modelling happens:

- `gold_dim_series` —> joins the cleaned lookups onto the series and derives
  `series_label` (`measure_text - sector_name - seasonal_text`). One row per
  series, codes and texts together. This is the join work that used to be in
  Silver.
- `gold_fact_productivity` —> the observations at `(series_id, year, period)`
  grain.
- `gold_population` —> population by year, the reference table.

Keeping Gold as clean key tables (rather than pre-baked answer tables) was a
deliberate choice, see below.

## Serving layer

The three questions are **not** answered inside the pipeline. They live in
`05_gold_serving.py` as `CREATE OR REPLACE VIEW` statements in the
`dataquest_serving` schema, on top of the Gold tables.

The reasoning: Gold should hold stable, reusable business entities. Metrics and
question-specific logic change far more often than the underlying model, and I
don't want every new metric to mean editing the pipeline and reprocessing data.
A view is cheap, it's instant to change, and putting the views in their own
schema means a read-only consumer can be granted access to just `dataquest_serving`
without touching the raw or curated tables.

Each question is written twice once in Spark SQL (the view, which is the object
that actually serves the answer) and once in PySpark (a documented equivalent
with `display()`), so the same result is reproducible either way.

**Q1 — US population mean/stddev, 2013–2018.** Straight aggregate over
`gold_population` filtered to the year range. `stddev` is the sample standard
deviation (Spark's default).

**Q2 — best year per series.** Sum `value` across the quarters
(`Q01`–`Q04`) per `(series_id, year)`, then take the top year per series with a
`ROW_NUMBER()` window. I deliberately exclude `Q05`, which is the annual average,
not a real quarter — including it would double-count. Ties break on the later
year. The series label comes from `gold_dim_series`.

**Q3 — PRS30006032 / Q01 with population.** Filter the fact to that series and
period and left-join `gold_population` on year. Left join on purpose: I want
every reported BLS year even if population is missing for it, rather than dropping
BLS rows to match the population range.

## Idempotency and reprocessing

There's no MERGE or upsert anywhere, and that's intentional. The BLS files are a
full republished snapshot, not a stream of deltas, and the ingestion overwrites
the landing files with the latest copy. Every table in the pipeline is a
materialized view — a full recompute from source on each run. So "latest wins" is
automatic: if BLS revises a value, the next run rebuilds the table and the old
value is simply gone. Duplicate keys inside a snapshot are handled by the
`dropDuplicates` in Silver.

A streaming/append design with CDC would buy me nothing here and would add
checkpoints and state for a dataset that's small and fully re-pullable. If the
source ever started sending incremental deltas, that's when I'd switch Bronze to
Auto Loader streaming tables.


## How to run it

1. Run `00_setup.py` to create the catalog, schemas and Volume.
2. Run `01_ingest.py` to pull the source files into the Volume.
3. Create one Lakeflow pipeline with:
   - Default catalog `rearc_dev`, serverless on.
   - Source = the three `Rearc_data_process/transformations` notebooks.
   - Config keys `catalog`, `raw_schema`, `bronze_schema`, `silver_schema`,
     `gold_schema`, `volume`.
   - Run it, the fully-qualified table names wire Bronze → Silver → Gold into one
     DAG automatically.
4. Run `05_gold_serving.py` to create the serving views and see the three answers.

## Trade-offs and things I'd do next

- **Thin Gold.** The Gold tables are close to Silver in shape. I justified the
  extra layer on the serving boundary and business naming, not on heavy
  transformation. For a dataset this size that's the right amount of modelling;
  I wouldn't add aggregation into Gold just to make it look busier.
- **No SCD history.** None of the three questions need "as-of" history, so I
  didn't build SCD2. If there were a real requirement to reproduce a past report
  after a BLS revision, the series dimension is the natural place for SCD2 and
  Gold would filter to the current rows — but adding it now would be
  over-engineering.
- **Audit table.** A small post-run table capturing row counts and load time per
  table would be a reasonable next addition. The pipeline UI already shows most
  of it, so I left it out to keep things lean.
- **DAB.** The notebooks are parameterised so wrapping them in a Databricks Asset
  Bundle (pipeline + orchestration job + dev/prod targets) is mostly additive and
  a sensible next step for real deployment.
