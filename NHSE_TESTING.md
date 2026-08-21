# NHS England testing guide: Splink-native pre-linkage assessment

## Purpose and status

This branch is an experimental extension of the Splink codebase. It adds
pre-linkage diagnostics that describe whether declared personal identifiers
are available and sufficiently distinguishing before a linkage model is fitted.
It is not an official Splink release and should not yet be used as a production
dependency.

The assessment does not estimate precision, recall or the number of successful
links. Those outcomes also depend on agreement between sources, blocking,
comparison functions, model estimation and the decision threshold.

## What is included

The public functions are exposed from `splink.exploratory` and cover:

- a dataset-level completeness, ambiguity, duplicate and record-ID overview;
- individual PII completeness, distinctness and value concentration;
- joint and per-field missingness;
- exact-signature ambiguity and ordered cardinality buckets;
- exact submitted-row duplicates;
- subgroup field quality, missingness, concentration and combination ambiguity;
- descriptive subgroup-minus-dataset gaps; and
- comparison with a previously approved aggregate reference profile.

The detailed function guide is in
`docs/topic_guides/data_preparation/pre_linkage_quality.md`. The worked notebook
is `examples/historical_50k_pre_linkage.ipynb`.

## Run the public example

Python 3.11 or 3.12 is recommended for testing. From the extracted repository:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[pyarrow]" jupyterlab pandas
python -m jupyter lab examples\historical_50k_pre_linkage.ipynb
```

The notebook uses Splink's public synthetic `historical_50k` dataset. It assesses
all 50,578 rows as one deduplication input; it does not split the data into
artificial left and right halves. The first sections declare field roles and
show the dataset-level linkage envelope before moving to field, combination and
subgroup detail.

To rerun the focused automated tests:

```powershell
python -m pip install pytest
python -m pytest tests\test_pre_linkage_quality.py `
  tests\test_pre_linkage_multiple_inputs.py `
  tests\test_extended_pre_linkage.py -q -p no:cacheprovider
```

## Substitute local data with DuckDB

The PII should remain in the approved processing environment. Register a pandas,
Arrow or DuckDB-compatible input, then call only the diagnostics required for
the assessment:

```python
from splink import DuckDBAPI
from splink.exploratory import (
    combination_size_distribution_chart,
    dataset_linkage_overview_chart,
    linkage_column_quality_chart,
)

db_api = DuckDBAPI()
records = db_api.register(local_dataframe)
pii_columns = ["first_name", "surname", "date_of_birth", "postcode"]

dataset_linkage_overview_chart(
    records,
    columns=pii_columns,
    record_id_column="record_id",
)
linkage_column_quality_chart(records, columns=pii_columns)
combination_size_distribution_chart(
    records,
    column_combinations=[
        pii_columns,
        ["surname", "date_of_birth", "postcode"],
        ["date_of_birth", "postcode"],
    ],
)
```

For two files to be linked, register both with the same database API and pass
`[left, right]`. The labels mean only the first and second inputs; neither is
assumed to be authoritative. Use `table_names_for_chart` to supply meaningful
source labels.

## Run with PySpark

On Databricks or another Linux Spark platform, install this branch on the cluster
and reuse the existing `SparkSession`:

```python
from splink import SparkAPI
from splink.exploratory import (
    dataset_linkage_overview_data,
    missingness_pattern_chart,
    subgroup_combination_ambiguity_chart,
)

db_api = SparkAPI(spark_session=spark)
records = db_api.register(source_spark_dataframe)
pii_columns = ["first_name", "surname", "date_of_birth", "postcode"]

overview = dataset_linkage_overview_data(
    records,
    columns=pii_columns,
    record_id_column="record_id",
)
missingness_pattern_chart(records, columns=pii_columns, top_n=12)
subgroup_combination_ambiguity_chart(
    records,
    column_combinations=[pii_columns, ["date_of_birth", "postcode"]],
    auxiliary_columns=["ethnic_group", "sex"],
)
```

Install Spark support for a local or managed cluster with:

```sh
python -m pip install -e ".[pyspark]"
```

The functions issue SQL through Splink's selected backend. Raw PII and exact
signatures are not returned by these APIs; aggregate records are collected for
tables and charts. Spark integration coverage is in
`tests/test_pre_linkage_spark.py` and should be run on Linux CI or the target
cluster:

```sh
python -m pytest tests/test_pre_linkage_spark.py -q -s -m spark \
  -p no:cacheprovider
```

The developer's Windows machine started Spark with Java 17, but local execution
was stopped by the organisation's policy blocking the unsigned Windows Hadoop
permission helper. This is an environment limitation, not a successful Spark
backend test. The Spark test therefore remains a required acceptance check for
the receiving Linux or Databricks environment.

## Governance boundary

The outputs omit observed identifier values and exact signatures. They still
contain aggregate counts, source labels and, when requested, auxiliary category
labels. Small groups and unusual distributions can be disclosive. This branch
does not apply small-cell suppression, automated interpretation or
project-specific normalisation. Apply the receiving environment's disclosure
controls before exporting any table, chart or profile.

Reference profiles should be approved aggregate artefacts produced with the same
column declarations and missing-value rules. A difference from a spine or prior
extract can reflect population and coverage change as well as data quality.

## Suggested acceptance checks

1. Run the historical notebook and confirm all charts render in the stated order.
2. Run the focused DuckDB tests above.
3. Run `test_pre_linkage_spark.py` on the target Spark platform.
4. Substitute a non-sensitive local fixture using the intended schema.
5. Check numerical agreement between DuckDB and Spark aggregate outputs.
6. Review subgroup outputs under the applicable disclosure policy.
7. Record Splink, Python, Spark and Java versions with test results.

Feedback should distinguish calculation defects, chart usability, backend
compatibility, disclosure requirements and requested interpretation. This makes
it possible to turn the prototype into small, reviewable upstream contributions.
