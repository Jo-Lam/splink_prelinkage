from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart
from splink.internals.combination_ambiguity import combination_ambiguity_data
from splink.internals.exact_duplicates import exact_duplicate_data
from splink.internals.pre_linkage_quality import linkage_column_quality_data
from splink.internals.splink_dataframe import SplinkDataFrame


class DatasetLinkageOverviewChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "dataset_linkage_overview.json"


def dataset_linkage_overview_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    record_id_column: str | None = None,
    duplicate_columns: Sequence[str] | None = None,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Summarise each input's structural linkage envelope.

    The result combines full-signature completeness and ambiguity, exact submitted
    duplicates, and optional record-ID checks. These are within-input constraints;
    they do not estimate cross-input overlap, agreement, precision or recall.
    """
    if not columns:
        raise ValueError("At least one linkage column must be provided.")
    combination = combination_ambiguity_data(
        splink_dataframe_or_dataframes,
        [columns],
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    duplicates = exact_duplicate_data(
        splink_dataframe_or_dataframes,
        duplicate_columns or columns,
        table_names_for_chart,
    )
    duplicate_index = {row["source_dataset"]: row for row in duplicates}
    id_index: dict[str, dict[str, Any]] = {}
    if record_id_column:
        id_rows = linkage_column_quality_data(
            splink_dataframe_or_dataframes,
            [record_id_column],
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
        id_index = {row["source_dataset"]: row for row in id_rows}

    output: list[dict[str, Any]] = []
    for dataset_order, row in enumerate(combination):
        duplicate = duplicate_index[row["source_dataset"]]
        identifier = id_index.get(row["source_dataset"])
        id_duplicate_records = None
        if identifier is not None:
            id_duplicate_records = round(
                float(identifier["repeated_record_share"])
                * int(identifier["observed_rows"])
            )
        output.append(
            {
                "source_dataset": row["source_dataset"],
                "dataset_order": dataset_order,
                "total_rows": int(row["total_rows"]),
                "linkage_columns": " + ".join(columns),
                "number_of_linkage_columns": len(columns),
                "complete_pii_share": float(row["complete_record_share"]),
                "usable_pii_share": float(row["usable_record_share"]),
                "unique_full_signature_share": float(row["unique_record_share"]),
                "ambiguous_full_signature_share": float(
                    row["ambiguous_record_share"]
                ),
                "unusable_full_signature_share": float(
                    row["unusable_record_share"]
                ),
                "maximum_one_to_one_assignment_share": float(
                    row["maximum_one_to_one_assignment_share"]
                ),
                "largest_signature_group": int(row["largest_signature_group"]),
                "exact_duplicate_record_share": float(
                    duplicate["duplicate_record_share"]
                ),
                "exact_duplicate_records": int(
                    duplicate["records_in_duplicate_groups"]
                ),
                "largest_exact_duplicate_group": int(
                    duplicate["largest_duplicate_group"]
                ),
                "record_id_column": record_id_column,
                "record_id_missing_rows": (
                    int(identifier["missing_rows"])
                    if identifier is not None
                    else None
                ),
                "record_id_duplicate_records": id_duplicate_records,
            }
        )
    return output


def dataset_linkage_overview_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    record_id_column: str | None = None,
    duplicate_columns: Sequence[str] | None = None,
    blank_as_null: bool = True,
) -> DatasetLinkageOverviewChart:
    """Plot the principal dataset-level structural linkage measures."""
    return DatasetLinkageOverviewChart(
        dataset_linkage_overview_data(
            splink_dataframe_or_dataframes,
            columns,
            table_names_for_chart,
            record_id_column=record_id_column,
            duplicate_columns=duplicate_columns,
            blank_as_null=blank_as_null,
        )
    )
