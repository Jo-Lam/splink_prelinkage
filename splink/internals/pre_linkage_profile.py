from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart
from splink.internals.combination_ambiguity import combination_ambiguity_data
from splink.internals.combination_distribution import (
    combination_size_distribution_data,
)
from splink.internals.dataset_linkage_overview import dataset_linkage_overview_data
from splink.internals.exact_duplicates import exact_duplicate_data
from splink.internals.missingness import (
    missingness_by_column_data,
    missingness_by_count_data,
    missingness_pattern_data,
    subgroup_missingness_pattern_data,
    subgroup_missingness_summary_data,
)
from splink.internals.pre_linkage_quality import linkage_column_quality_data
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.subgroup_combination import (
    subgroup_combination_ambiguity_data,
)
from splink.internals.subgroup_quality import (
    _subgroup_equity_gap_records,
    subgroup_linkage_column_quality_data,
)
from splink.internals.subgroup_value_distribution import (
    subgroup_value_concentration_data,
    subgroup_value_dominance_distribution_data,
)
from splink.internals.value_distribution import (
    value_concentration_data,
    value_dominance_distribution_data,
)


class PreLinkageReferenceComparisonChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "pre_linkage_reference_comparison.json"


def build_pre_linkage_profile(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    *,
    columns: Sequence[str],
    column_combinations: Sequence[Sequence[str]],
    auxiliary_columns: Sequence[str] = (),
    duplicate_columns: Sequence[str] | None = None,
    record_id_column: str | None = None,
    table_names_for_chart: Sequence[str] | None = None,
    blank_as_null: bool = True,
    maximum_curve_points: int = 500,
) -> dict[str, Any]:
    """Build a reusable aggregate profile without producing a report."""
    common = {
        "splink_dataframe_or_dataframes": splink_dataframe_or_dataframes,
        "columns": columns,
        "table_names_for_chart": table_names_for_chart,
        "blank_as_null": blank_as_null,
    }
    profile: dict[str, Any] = {
        "profile_type": "splink_pre_linkage_profile",
        "columns": list(columns),
        "column_combinations": [list(item) for item in column_combinations],
        "auxiliary_columns": list(auxiliary_columns),
        "dataset_overview": dataset_linkage_overview_data(
            splink_dataframe_or_dataframes,
            columns,
            table_names_for_chart,
            record_id_column=record_id_column,
            duplicate_columns=duplicate_columns,
            blank_as_null=blank_as_null,
        ),
        "column_quality": linkage_column_quality_data(**common),
        "value_concentration": value_concentration_data(
            **common, maximum_points=maximum_curve_points
        ),
        "value_dominance_distribution": value_dominance_distribution_data(
            **common
        ),
        "missingness_by_column": missingness_by_column_data(**common),
        "missingness_by_count": missingness_by_count_data(**common),
        "missingness_patterns": missingness_pattern_data(**common),
        "combination_ambiguity": combination_ambiguity_data(
            splink_dataframe_or_dataframes,
            column_combinations,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        ),
        "combination_size_distribution": combination_size_distribution_data(
            splink_dataframe_or_dataframes,
            column_combinations,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        ),
        "exact_duplicates": exact_duplicate_data(
            splink_dataframe_or_dataframes,
            duplicate_columns or columns,
            table_names_for_chart,
        ),
    }
    if auxiliary_columns:
        profile["subgroup_missingness_patterns"] = (
            subgroup_missingness_pattern_data(
                splink_dataframe_or_dataframes,
                columns,
                auxiliary_columns,
                table_names_for_chart,
                blank_as_null=blank_as_null,
            )
        )
        profile["subgroup_missingness_summary"] = (
            subgroup_missingness_summary_data(
                splink_dataframe_or_dataframes,
                columns,
                auxiliary_columns,
                table_names_for_chart,
                blank_as_null=blank_as_null,
            )
        )
        subgroup_fields = subgroup_linkage_column_quality_data(
            splink_dataframe_or_dataframes,
            columns,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
        subgroup_combinations = subgroup_combination_ambiguity_data(
            splink_dataframe_or_dataframes,
            column_combinations,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
        profile["subgroup_linkage_column_quality"] = subgroup_fields
        profile["subgroup_combination_ambiguity"] = subgroup_combinations
        profile["subgroup_equity_gaps"] = _subgroup_equity_gap_records(
            subgroup_fields,
            subgroup_combinations,
        )
        profile["subgroup_value_concentration"] = (
            subgroup_value_concentration_data(
                splink_dataframe_or_dataframes,
                columns,
                auxiliary_columns,
                table_names_for_chart,
                blank_as_null=blank_as_null,
                maximum_points=maximum_curve_points,
            )
        )
        profile["subgroup_value_dominance_distribution"] = (
            subgroup_value_dominance_distribution_data(
                splink_dataframe_or_dataframes,
                columns,
                auxiliary_columns,
                table_names_for_chart,
                blank_as_null=blank_as_null,
            )
        )
    return profile


_COMPARISON_DEFINITIONS = {
    "dataset_overview": (
        (),
        (
            "complete_pii_share",
            "usable_pii_share",
            "unique_full_signature_share",
            "ambiguous_full_signature_share",
            "maximum_one_to_one_assignment_share",
            "exact_duplicate_record_share",
        ),
    ),
    "column_quality": (
        ("column_name",),
        (
            "usable_completeness",
            "distinct_value_rate",
            "singleton_record_share",
            "repeated_record_share",
            "largest_value_share",
            "top_5pct_values_record_share",
        ),
    ),
    "missingness_by_column": (
        ("column_name",),
        ("missing_record_share",),
    ),
    "missingness_patterns": (
        ("missing_fields",),
        ("record_share",),
    ),
    "combination_ambiguity": (
        ("column_combination",),
        (
            "complete_record_share",
            "unique_record_share",
            "ambiguous_record_share",
            "unusable_record_share",
            "maximum_one_to_one_assignment_share",
        ),
    ),
    "combination_size_distribution": (
        ("column_combination", "cardinality_bucket"),
        ("usable_record_share",),
    ),
    "exact_duplicates": (
        ("columns_used",),
        ("duplicate_record_share",),
    ),
    "subgroup_missingness_summary": (
        ("auxiliary_column", "auxiliary_value"),
        ("any_missing_share", "most_common_incomplete_pattern_share"),
    ),
    "subgroup_linkage_column_quality": (
        ("auxiliary_column", "auxiliary_value", "column_name"),
        (
            "group_record_share",
            "usable_completeness",
            "distinct_value_rate",
            "repeated_record_share",
            "largest_value_share",
            "top_5pct_values_record_share",
        ),
    ),
    "subgroup_combination_ambiguity": (
        ("auxiliary_column", "auxiliary_value", "column_combination"),
        (
            "group_record_share",
            "complete_record_share",
            "unique_record_share",
            "ambiguous_record_share",
            "unusable_record_share",
            "maximum_one_to_one_assignment_share",
        ),
    ),
    "subgroup_equity_gaps": (
        ("auxiliary_column", "auxiliary_value", "assessment_level", "item"),
        ("largest_adverse_gap", "adverse_gap_magnitude"),
    ),
}


def _dataset_names(profile: Mapping[str, Any]) -> list[str]:
    rows = profile.get("column_quality", [])
    return list(dict.fromkeys(str(row["source_dataset"]) for row in rows))


def _profile_index(rows, dataset, keys):
    return {
        tuple(str(row.get(key, "")) for key in keys): row
        for row in rows
        if str(row.get("source_dataset")) == dataset
    }


def compare_pre_linkage_profiles(
    current_profile: Mapping[str, Any],
    reference_profile: Mapping[str, Any],
    *,
    current_dataset: str | None = None,
    reference_dataset: str | None = None,
    current_label: str = "Current",
    reference_label: str = "Reference",
) -> list[dict[str, Any]]:
    """Compare aggregate profiles; positive differences are current minus
    reference."""
    current_names = _dataset_names(current_profile)
    reference_names = _dataset_names(reference_profile)
    if not current_names or not reference_names:
        raise ValueError("Both profiles must contain column_quality records.")
    current_dataset = current_dataset or current_names[0]
    reference_dataset = reference_dataset or reference_names[0]
    if current_dataset not in current_names:
        raise ValueError(f"Unknown current_dataset: {current_dataset}")
    if reference_dataset not in reference_names:
        raise ValueError(f"Unknown reference_dataset: {reference_dataset}")

    output: list[dict[str, Any]] = []
    for section, (keys, metrics) in _COMPARISON_DEFINITIONS.items():
        current_index = _profile_index(
            current_profile.get(section, []), current_dataset, keys
        )
        reference_index = _profile_index(
            reference_profile.get(section, []), reference_dataset, keys
        )
        for item_key in sorted(set(current_index) | set(reference_index)):
            current_row = current_index.get(item_key, {})
            reference_row = reference_index.get(item_key, {})
            for metric in metrics:
                current_value = current_row.get(metric)
                reference_value = reference_row.get(metric)
                difference = None
                if current_value is not None and reference_value is not None:
                    difference = float(current_value) - float(reference_value)
                output.append(
                    {
                        "comparison_section": section,
                        "item": " | ".join(item_key),
                        "metric": metric,
                        "current_dataset": current_dataset,
                        "reference_dataset": reference_dataset,
                        "current_label": current_label,
                        "reference_label": reference_label,
                        "current_value": (
                            float(current_value) if current_value is not None else None
                        ),
                        "reference_value": (
                            float(reference_value)
                            if reference_value is not None
                            else None
                        ),
                        "difference": difference,
                    }
                )
    return output


def pre_linkage_reference_comparison_chart(
    current_profile: Mapping[str, Any],
    reference_profile: Mapping[str, Any],
    **kwargs,
) -> PreLinkageReferenceComparisonChart:
    """Chart aligned metric differences between current and reference profiles."""
    return PreLinkageReferenceComparisonChart(
        compare_pre_linkage_profiles(current_profile, reference_profile, **kwargs)
    )
