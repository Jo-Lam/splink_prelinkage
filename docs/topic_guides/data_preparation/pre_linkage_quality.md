# Assessing linkage-column quality before modelling

Pre-linkage assessment describes whether the submitted columns provide enough
usable and distinguishing information for record linkage. It does not require a
trained Splink model or known matching pairs.

These diagnostics help answer two questions:

1. Are candidate linkage columns sufficiently complete and varied?
2. Which exact column combinations leave many records indistinguishable?

They describe structural constraints in the input data. They do not estimate
precision, recall or the number of records that will link successfully. Those
outcomes also depend on agreement between sources, blocking, the linkage model
and the decision threshold.

## Register the input data

Splink 5 exploratory functions accept registered `SplinkDataFrame` objects. The
input remains in the selected database backend.

```python
from splink import DuckDBAPI
from splink.exploratory import (
    combination_ambiguity_chart,
    linkage_column_quality_chart,
)

db_api = DuckDBAPI()
records = db_api.register(my_records)
```

For a link-only project, register each party's table with the same database API:

```python
left = db_api.register(party_a_records)
right = db_api.register(party_b_records)
```

Here, `left` and `right` indicate the two inputs to be linked. They do not imply
that either table is authoritative.

## Choose the function for the question

Functions ending in `_data` return aggregate Python records suitable for tables,
audit files or custom visualisation. Functions ending in `_chart` calculate the
same evidence and return a Splink chart. Identifier values and exact signatures
are not returned, although auxiliary category labels are returned when subgroup
assessment is requested.

| Function or function pair | What it tells you | What it does not tell you |
|---|---|---|
| `dataset_linkage_overview_data/chart` | Whole-input completeness, usability, full-signature ambiguity, exact duplicates and optional record-ID problems | Expected match rate or accuracy against another source |
| `linkage_column_quality_data/chart` | Completeness, distinctness, singleton/repeated values and concentration for each PII field | Whether values agree across parties |
| `missingness_by_column_data/chart` | How often each declared PII is absent | Whether missing values are recoverable elsewhere |
| `missingness_pattern_data/chart` | Which PII fields are absent together and the most common patterns | The cause of co-missingness |
| `missingness_by_count_data` | Distribution of rows by number of missing PII fields | Which particular fields are missing |
| `value_concentration_data/chart` | Cumulative concentration from most to least frequent PII values | The observed values themselves |
| `value_dominance_distribution_data/chart` | Full distribution of value frequencies | Whether common values generate false links in a fitted model |
| `combination_ambiguity_data/chart` | Unique, ambiguous and unusable rows under each exact PII signature | Cross-file candidate counts or realised linkage accuracy |
| `combination_size_distribution_data/chart` | Share of usable records in exact-signature groups of size `1` through `100+` | Whether a larger group contains true duplicates or different people |
| `exact_duplicate_data/chart` | Exact submitted duplicates across declared columns | Whether duplicates are legitimate repeated events or ingestion errors |
| `subgroup_missingness_pattern_data` | Complete co-missingness distribution within auxiliary groups | Statistical significance or causal inequity |
| `subgroup_missingness_summary_data/chart` | Any-PII-missing share and leading incomplete pattern by group | Realised subgroup linkage error |
| `subgroup_linkage_column_quality_data/chart` | Field quality and subgroup-minus-dataset gaps | Whether observed gaps persist after linkage |
| `subgroup_combination_ambiguity_data/chart` | Exact-signature distinguishability within each group | A fairness or discrimination test |
| `subgroup_equity_gap_data/chart` | Largest adverse structural gap and a descriptive review category | Inferential uncertainty or evidence of bias |
| `subgroup_value_concentration_data/chart` | Cumulative value concentration within groups | Identifier values or match outcomes |
| `subgroup_value_dominance_distribution_data/chart` | Full frequency distribution within groups | Whether group differences are caused by data collection or population composition |
| `build_pre_linkage_profile` | Serialisable collection of all configured aggregate diagnostics | A standalone report or disclosure approval |
| `compare_pre_linkage_profiles` | Current-minus-reference differences in aligned aggregate metrics | Whether the reference is correct or comparable |
| `pre_linkage_reference_comparison_chart` | Compact view of aligned current and reference metrics | Distribution shape |
| `reference_value_concentration_chart` | Direct overlay of current and reference concentration curves | Population-standardised differences |
| `reference_value_dominance_distribution_chart` | Direct overlay of current and reference frequency distributions | Size-independent differences |

## Start with the dataset-level overview

```python
from splink.exploratory import (
    dataset_linkage_overview_chart,
    dataset_linkage_overview_data,
)

overview = dataset_linkage_overview_data(
    records,
    columns=["first_name", "surname", "date_of_birth", "postcode"],
    record_id_column="record_id",
    duplicate_columns=["first_name", "surname", "date_of_birth", "postcode"],
)
dataset_linkage_overview_chart(
    records,
    columns=["first_name", "surname", "date_of_birth", "postcode"],
    record_id_column="record_id",
)
```

Read the measures in order:

1. **All PII complete** is the share available to an exact rule requiring every
   declared field.
2. **At least one PII usable** identifies rows for which some identifier evidence
   remains.
3. **Rows with a unique full signature** identifies rows not tied with another
   row on the exact full-PII combination.
4. **Maximum distinct 1:1 assignments** allows one assignment for every usable
   exact signature. It is a structural ceiling, not predicted linkage quality.

Missing or repeated record IDs are ingestion and row-identification checks. Exact
duplicate rows concern the declared `duplicate_columns`; neither establishes
whether repeated rows represent the same person.

## Assess individual columns

```python
quality_chart = linkage_column_quality_chart(
    [left, right],
    columns=["first_name", "surname", "date_of_birth", "postcode"],
    table_names_for_chart=["Party A", "Party B"],
)
quality_chart
```

The chart reports:

- **Usable completeness**: the share of rows with a non-null, non-blank value.
- **Distinct-value rate**: distinct values divided by observed rows.
- **Singleton-record share**: the share of observed rows whose value occurs once.
- **Repeated-record share**: the share of observed rows whose value occurs more
  than once.
- **Largest-value share**: the share of observed rows occupied by the most common
  value.
- **Top-5% concentration:** the share of observed rows occupied by the most
  frequent 5% of distinct values, with at least one value included. This
  complements the largest-value statistic and the full distribution charts.

These measures should be interpreted together. For example, a complete column
with few distinct values may be useful as part of a combination, but is unlikely
to distinguish records on its own.

Set `blank_as_null=False` only when blank strings are meaningful values. This
option does not recognise project-specific placeholder values such as
`01/01/1900`; those require explicit standardisation before assessment.

The underlying aggregate records are available separately:

```python
from splink.exploratory import linkage_column_quality_data

quality_records = linkage_column_quality_data(
    [left, right],
    columns=["first_name", "surname", "date_of_birth", "postcode"],
    table_names_for_chart=["Party A", "Party B"],
)
```

## Assess exact-combination ambiguity

```python
ambiguity_chart = combination_ambiguity_chart(
    [left, right],
    column_combinations=[
        ["first_name", "surname", "date_of_birth", "postcode"],
        ["surname", "date_of_birth", "postcode"],
        ["date_of_birth", "postcode"],
    ],
    table_names_for_chart=["Party A", "Party B"],
)
ambiguity_chart
```

Each combination is evaluated as an exact signature within each input table:

- A **unique signature** occurs on one row.
- An **ambiguous signature** occurs on multiple rows.
- A row has **no usable value** when every column in the combination is missing.
- **Maximum one-to-one assignment share** is the number of distinct usable
  signatures divided by all rows. It is a structural upper bound on the number
  of rows that could receive distinct assignments from that signature alone,
  not an accuracy estimate.

Progressively removing columns shows how ambiguity increases as an exact
blocking key is relaxed. Candidate combinations should subsequently be tested
with Splink's blocking-analysis functions because low within-table ambiguity
does not guarantee manageable cross-table comparison counts.

## Examine the complete value distribution

A largest-value statistic alone cannot distinguish a field with one unusually
common value from a field containing many moderately common values. Use both
distribution views:

```python
from splink.exploratory import (
    value_concentration_chart,
    value_dominance_distribution_chart,
)

value_concentration_chart(
    records,
    columns=["first_name", "surname", "postcode"],
)
value_dominance_distribution_chart(
    records,
    columns=["first_name", "surname", "postcode"],
)
```

The cumulative concentration curve ranks values from most to least frequent
and plots cumulative record share against cumulative distinct-value share. The
dominance distribution reports how many values, and how many records, occur at
each frequency. Neither output contains the observed identifier values.

## Examine missingness jointly and by subgroup

```python
from splink.exploratory import (
    missingness_by_column_chart,
    missingness_by_count_data,
    missingness_pattern_chart,
    missingness_pattern_data,
    subgroup_missingness_chart,
    subgroup_value_concentration_chart,
    subgroup_value_dominance_distribution_chart,
)

missingness_by_column_chart(records, columns=linkage_columns)
patterns = missingness_pattern_data(records, columns=linkage_columns)
counts = missingness_by_count_data(records, columns=linkage_columns)
missingness_pattern_chart(records, columns=linkage_columns, top_n=12)
subgroup_missingness_chart(
    records,
    columns=linkage_columns,
    auxiliary_columns=["sex", "source_system"],
)
subgroup_value_concentration_chart(
    records,
    columns=["first_name", "surname", "postcode"],
    auxiliary_columns=["sex"],
)
subgroup_value_dominance_distribution_chart(
    records,
    columns=["first_name", "surname", "postcode"],
    auxiliary_columns=["sex"],
)
```

`missingness_pattern_data` returns every co-missingness pattern, including the
most common pattern as the first record for each dataset. `missingness_by_count`
groups rows by the number of missing linkage columns. Auxiliary stratification
is optional and accepts one or more declared auxiliary columns.
The subgroup concentration and dominance charts apply the same value-frequency
diagnostics within each declared auxiliary group. Auxiliary values are included
in these outputs, while observed identifier values remain excluded.

## Assess subgroup field quality and combination ambiguity

```python
from splink.exploratory import (
    subgroup_combination_ambiguity_chart,
    subgroup_equity_gap_chart,
    subgroup_linkage_column_quality_chart,
)

subgroup_linkage_column_quality_chart(
    records,
    columns=linkage_columns,
    auxiliary_columns=["sex", "source_system"],
)
subgroup_combination_ambiguity_chart(
    records,
    column_combinations=column_combinations,
    auxiliary_columns=["sex", "source_system"],
)
subgroup_equity_gap_chart(
    records,
    columns=linkage_columns,
    column_combinations=column_combinations,
    auxiliary_columns=["sex", "source_system"],
)
```

Subgroup field quality reports completeness, distinctness, repeated-value share
and top-5% concentration within each declared group. Subgroup combination
ambiguity applies exact PII signatures within each group. Every metric includes
a subgroup-minus-dataset gap.

The equity-gap chart selects the most adverse direction across these metrics.
Lower completeness, distinctness or one-to-one distinguishability and higher
repetition, concentration or ambiguity are treated as adverse. Flags at 2, 5
and 10 percentage points are descriptive triage thresholds inherited from the
standalone assessment; they are not statistical tests and do not demonstrate
discriminatory linkage outcomes. Small groups can be unstable and no automatic
suppression is applied in this implementation.

## Inspect combination sizes and exact duplicate rows

```python
from splink.exploratory import (
    combination_size_distribution_chart,
    exact_duplicate_chart,
)

combination_size_distribution_chart(
    records,
    column_combinations=column_combinations,
)
exact_duplicate_chart(records, columns=submitted_columns)
```

The combination-size distribution places exact signature groups in buckets
`1`, `2`, `3`, `4`, `5-9`, `10-19`, `20-49`, `50-99` and `100+`. The exact
duplicate assessment reports duplicate groups, records within those groups,
excess rows and the largest group. Exact duplicates use the submitted values;
no case conversion, trimming or other normalisation is applied.

The cardinality chart uses a fixed ordered scale: `1`, `2`, `3`, `4`, `5-9`,
`10-19`, `20-49`, `50-99`, `100+`. Each horizontal bar represents one declared
PII combination and sums to the share of usable records. The combinations retain
the order supplied by the caller, so place the strictest or primary signature
first and progressively relaxed alternatives afterwards.

## Build and compare reusable profiles

```python
from splink.exploratory import (
    build_pre_linkage_profile,
    compare_pre_linkage_profiles,
    pre_linkage_reference_comparison_chart,
    reference_value_concentration_chart,
    reference_value_dominance_distribution_chart,
)

current_profile = build_pre_linkage_profile(
    records,
    columns=linkage_columns,
    column_combinations=column_combinations,
    auxiliary_columns=["sex"],
    duplicate_columns=submitted_columns,
)
comparison = compare_pre_linkage_profiles(
    current_profile,
    reference_profile,
    current_label="Current submission",
    reference_label="Previous spine extract",
)
pre_linkage_reference_comparison_chart(
    current_profile,
    reference_profile,
)
reference_value_concentration_chart(
    current_profile,
    reference_profile,
)
reference_value_dominance_distribution_chart(
    current_profile,
    reference_profile,
)
```

A reference profile is another aggregate dictionary produced by
`build_pre_linkage_profile`, for example from an earlier extract or an approved
reference population. Positive differences are always current minus reference.
Profiles should use the same column declarations and missing-value rules.
Reference comparison is descriptive: a difference may reflect population or
coverage change as well as data-quality change.
The two overlay charts compare the complete aggregate concentration curves and
frequency distributions directly. They remain sensitive to differences in file
size and population composition.

The profile builder returns aggregate records and does not create a composite
report. Charts and data functions remain independently callable, so a project
can select only the diagnostics it needs. This initial implementation does not
apply project-specific normalisation, automated interpretation or small-cell
suppression.

## Privacy and disclosure

The functions return column names and aggregate counts, but not observed
identifier values or exact signatures. Aggregate outputs can nevertheless reveal
information about small groups or unusual distributions. Apply the disclosure
rules required by the operating environment before sharing them.

Running these functions in Splink does not itself provide private set
intersection, privacy-preserving record linkage or secure multi-party
computation. If parties cannot place their inputs in the same trusted processing
environment, each party can run the assessment locally and exchange approved
aggregates, but the resulting comparison is less informative than joint
record-level assessment.
