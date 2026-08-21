# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Pre-linkage assessment of the historical 50k dataset
#
# This example assesses one dataset before deduplication. It asks whether the
# available linkage columns are complete and distinguishing, and how exact
# signature ambiguity changes as columns are removed. It does not fit a linkage
# model or estimate precision and recall.

# %%
import csv
import json
from pathlib import Path

from splink import DuckDBAPI, splink_datasets
from splink.exploratory import (
    combination_ambiguity_chart,
    combination_ambiguity_data,
    linkage_column_quality_chart,
    linkage_column_quality_data,
)

OUTPUT_DIR = Path("example_outputs/historical_50k_pre_linkage")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# Splink calls this the historical 50k dataset. It contains 50,578 rows: the
# name describes its approximate rather than exact size. We use the complete
# table as one input because this is a deduplication assessment.

# %%
db_api = DuckDBAPI()
historical_50k = splink_datasets.historical_50k
records = db_api.register(historical_50k)

print(f"Rows: {historical_50k.num_rows:,}")
print(f"Columns: {', '.join(historical_50k.column_names)}")

# %% [markdown]
# ## Individual linkage columns
#
# `usable_completeness` treats null, empty and whitespace-only strings as
# unavailable. The concentration measures report counts and shares, but never
# disclose the names, dates, postcodes or other observed values.

# %%
linkage_columns = [
    "first_name",
    "surname",
    "dob",
    "postcode_fake",
    "birth_place",
    "occupation",
]

column_quality = linkage_column_quality_data(
    records,
    columns=linkage_columns,
    table_names_for_chart=["Historical 50k"],
)
column_chart = linkage_column_quality_chart(
    records,
    columns=linkage_columns,
    table_names_for_chart=["Historical 50k"],
)
column_chart

# %% [markdown]
# ## Exact-combination ambiguity
#
# The first signature uses all four person identifiers. Later signatures remove
# fields to illustrate the ambiguity introduced by progressively relaxed exact
# keys. These are diagnostics, not recommended blocking rules by themselves.

# %%
column_combinations = [
    ["first_name", "surname", "dob", "postcode_fake"],
    ["surname", "dob", "postcode_fake"],
    ["first_name", "dob", "postcode_fake"],
    ["dob", "postcode_fake"],
    ["surname", "dob"],
    ["postcode_fake"],
]

combination_ambiguity = combination_ambiguity_data(
    records,
    column_combinations=column_combinations,
    table_names_for_chart=["Historical 50k"],
)
ambiguity_chart = combination_ambiguity_chart(
    records,
    column_combinations=column_combinations,
    table_names_for_chart=["Historical 50k"],
)
ambiguity_chart

# %% [markdown]
# ## Save reproducible outputs
#
# The two HTML files are standalone Splink charts. The CSV and JSON files expose
# the same aggregate evidence for audit or later report composition.

# %%
def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


write_csv(OUTPUT_DIR / "linkage_column_quality.csv", column_quality)
write_csv(OUTPUT_DIR / "combination_ambiguity.csv", combination_ambiguity)

with (OUTPUT_DIR / "pre_linkage_metrics.json").open("w", encoding="utf-8") as handle:
    json.dump(
        {
            "dataset": "Splink historical 50k",
            "records": historical_50k.num_rows,
            "interpretation": (
                "Structural pre-linkage diagnostics, not estimates of precision, "
                "recall or realised links."
            ),
            "linkage_column_quality": column_quality,
            "combination_ambiguity": combination_ambiguity,
        },
        handle,
        indent=2,
    )

column_chart.save_offline_chart(
    OUTPUT_DIR / "linkage_column_quality.html",
    overwrite=True,
    print_msg=False,
)
ambiguity_chart.save_offline_chart(
    OUTPUT_DIR / "combination_ambiguity.html",
    overwrite=True,
    print_msg=False,
)

print(f"Outputs written to {OUTPUT_DIR.resolve()}")
