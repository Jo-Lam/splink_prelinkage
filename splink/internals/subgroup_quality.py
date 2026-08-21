from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart
from splink.internals.misc import join_sql_with_union_all
from splink.internals.pipeline import CTEPipeline
from splink.internals.pre_linkage_quality import (
    _apply_table_names,
    _enqueue_concatenated_inputs,
    _input_columns,
    _normalised_value_sql,
    _sql_literal,
    _validate_table_names,
    linkage_column_quality_data,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import (
    get_db_api_from_inputs,
    splink_dataframes_to_dict,
)
from splink.internals.subgroup_combination import (
    subgroup_combination_ambiguity_data,
)


class SubgroupLinkageColumnQualityChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "subgroup_linkage_column_quality.json"


class SubgroupEquityGapChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "subgroup_equity_gap.json"


def subgroup_linkage_column_quality_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Calculate linkage-column quality within auxiliary-variable strata."""
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    first = next(iter(frames.values()))
    fields = _input_columns(columns, first, db_api)
    auxiliary = _input_columns(auxiliary_columns, first, db_api)
    if not fields:
        raise ValueError("At least one linkage column must be provided.")
    if not auxiliary:
        raise ValueError("At least one auxiliary column must be provided.")
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    queries: list[str] = []

    for auxiliary_order, group_column in enumerate(auxiliary):
        group_value = _normalised_value_sql(group_column.name, blank_as_null)
        group_label = group_column.unquote().name
        for column_order, field in enumerate(fields):
            value = _normalised_value_sql(field.name, blank_as_null)
            field_label = field.unquote().name
            queries.append(
                f"""
                select * from (with values_ as (
                    select {source} as source_dataset,
                           coalesce({group_value}, '[MISSING]')
                               as auxiliary_value,
                           {value} as value
                    from __splink__df_pre_linkage_inputs
                ), group_stats as (
                    select source_dataset, auxiliary_value,
                           count(*) as group_records,
                           count(value) as observed_rows
                    from values_
                    group by source_dataset, auxiliary_value
                ), value_counts as (
                    select source_dataset, auxiliary_value, value,
                           count(*) as value_count
                    from values_
                    where value is not null
                    group by source_dataset, auxiliary_value, value
                ), ranked_value_counts as (
                    select source_dataset, auxiliary_value, value, value_count,
                           row_number() over (
                               partition by source_dataset, auxiliary_value
                               order by value_count desc, value
                           ) as value_rank,
                           count(*) over (
                               partition by source_dataset, auxiliary_value
                           ) as distinct_value_count
                    from value_counts
                ), value_stats as (
                    select source_dataset, auxiliary_value,
                           count(*) as distinct_values,
                           sum(case when value_count = 1
                               then value_count else 0 end) as singleton_records,
                           sum(case when value_count > 1
                               then value_count else 0 end) as repeated_records,
                           max(value_count) as largest_value_count,
                           sum(case when value_rank <= greatest(
                                   1, ceil(distinct_value_count * 0.05)
                               ) then value_count else 0 end)
                               as top_5pct_values_record_count
                    from ranked_value_counts
                    group by source_dataset, auxiliary_value
                )
                select g.source_dataset,
                       {_sql_literal(group_label)} as auxiliary_column,
                       {auxiliary_order} as auxiliary_order,
                       g.auxiliary_value,
                       {_sql_literal(field_label)} as column_name,
                       {column_order} as column_order,
                       g.group_records, g.observed_rows,
                       g.group_records - g.observed_rows as missing_rows,
                       cast(g.observed_rows * 1.0 / g.group_records as float)
                           as usable_completeness,
                       coalesce(v.distinct_values, 0) as distinct_values,
                       cast(coalesce(v.distinct_values, 0) * 1.0
                           / nullif(g.observed_rows, 0) as float)
                           as distinct_value_rate,
                       cast(coalesce(v.singleton_records, 0) * 1.0
                           / nullif(g.observed_rows, 0) as float)
                           as singleton_record_share,
                       cast(coalesce(v.repeated_records, 0) * 1.0
                           / nullif(g.observed_rows, 0) as float)
                           as repeated_record_share,
                       coalesce(v.largest_value_count, 0) as largest_value_count,
                       cast(coalesce(v.largest_value_count, 0) * 1.0
                           / nullif(g.observed_rows, 0) as float)
                           as largest_value_share,
                       coalesce(v.top_5pct_values_record_count, 0)
                           as top_5pct_values_record_count,
                       cast(coalesce(v.top_5pct_values_record_count, 0) * 1.0
                           / nullif(g.observed_rows, 0) as float)
                           as top_5pct_values_record_share
                from group_stats g
                left join value_stats v
                  on g.source_dataset = v.source_dataset
                 and g.auxiliary_value = v.auxiliary_value
                ) as subgroup_column_quality
                """
            )

    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_subgroup_linkage_column_quality",
    )
    records = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    records = _apply_table_names(records, names)
    overall = linkage_column_quality_data(
        splink_dataframe_or_dataframes,
        columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    overall_index = {
        (row["source_dataset"], row["column_name"]): row for row in overall
    }
    group_sizes = {
        (
            row["source_dataset"],
            row["auxiliary_column"],
            row["auxiliary_value"],
        ): int(row["group_records"])
        for row in records
    }
    group_totals: dict[tuple[str, str], int] = {}
    for (source_name, auxiliary_name, _), size in group_sizes.items():
        key = (source_name, auxiliary_name)
        group_totals[key] = group_totals.get(key, 0) + size
    gap_metrics = (
        "usable_completeness",
        "distinct_value_rate",
        "singleton_record_share",
        "repeated_record_share",
        "largest_value_share",
        "top_5pct_values_record_share",
    )
    for row in records:
        baseline = overall_index[(row["source_dataset"], row["column_name"])]
        total = group_totals[(row["source_dataset"], row["auxiliary_column"])]
        row["group_record_share"] = row["group_records"] / total if total else 0.0
        for metric in gap_metrics:
            subgroup_value = float(row[metric]) if row[metric] is not None else 0.0
            baseline_value = (
                float(baseline[metric]) if baseline[metric] is not None else 0.0
            )
            row[metric] = subgroup_value
            row[f"{metric}_gap"] = subgroup_value - baseline_value
    return records


def subgroup_linkage_column_quality_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> SubgroupLinkageColumnQualityChart:
    """Chart PII completeness and distinctness within auxiliary groups."""
    return SubgroupLinkageColumnQualityChart(
        subgroup_linkage_column_quality_data(
            splink_dataframe_or_dataframes,
            columns,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
    )


def _flag_level(magnitude: float) -> str:
    if magnitude >= 0.10:
        return "priority review"
    if magnitude >= 0.05:
        return "review"
    if magnitude >= 0.02:
        return "monitor"
    return "no material aggregate flag"


def _subgroup_equity_gap_records(
    field_quality: Sequence[dict[str, Any]],
    combination_quality: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in field_quality:
        components = {
            "completeness": float(row["usable_completeness_gap"]),
            "distinct-value rate": float(row["distinct_value_rate_gap"]),
            "repeated-value share": -float(row["repeated_record_share_gap"]),
            "top-5% concentration": -float(
                row["top_5pct_values_record_share_gap"]
            ),
        }
        metric, adverse_gap = min(components.items(), key=lambda item: item[1])
        adverse_gap = min(0.0, adverse_gap)
        output.append(
            {
                "source_dataset": row["source_dataset"],
                "auxiliary_column": row["auxiliary_column"],
                "auxiliary_value": row["auxiliary_value"],
                "assessment_level": "individual PII",
                "item": row["column_name"],
                "group_records": row["group_records"],
                "group_record_share": row["group_record_share"],
                "adverse_metric": metric,
                "largest_adverse_gap": adverse_gap,
                "adverse_gap_magnitude": abs(adverse_gap),
                "flag_level": _flag_level(abs(adverse_gap)),
            }
        )
    for row in combination_quality:
        components = {
            "complete PII combination": float(row["complete_record_share_gap"]),
            "unique-signature share": float(row["unique_record_share_gap"]),
            "ambiguous-signature share": -float(
                row["ambiguous_record_share_gap"]
            ),
            "maximum 1:1 assignment share": float(
                row["maximum_one_to_one_assignment_share_gap"]
            ),
        }
        metric, adverse_gap = min(components.items(), key=lambda item: item[1])
        adverse_gap = min(0.0, adverse_gap)
        output.append(
            {
                "source_dataset": row["source_dataset"],
                "auxiliary_column": row["auxiliary_column"],
                "auxiliary_value": row["auxiliary_value"],
                "assessment_level": "combined PII",
                "item": row["column_combination"],
                "group_records": row["group_records"],
                "group_record_share": row["group_record_share"],
                "adverse_metric": metric,
                "largest_adverse_gap": adverse_gap,
                "adverse_gap_magnitude": abs(adverse_gap),
                "flag_level": _flag_level(abs(adverse_gap)),
            }
        )
    return sorted(output, key=lambda row: row["largest_adverse_gap"])


def subgroup_equity_gap_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    column_combinations: Sequence[Sequence[str]],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Summarise adverse subgroup gaps relative to each dataset average."""
    fields = subgroup_linkage_column_quality_data(
        splink_dataframe_or_dataframes,
        columns,
        auxiliary_columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    combinations = subgroup_combination_ambiguity_data(
        splink_dataframe_or_dataframes,
        column_combinations,
        auxiliary_columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    return _subgroup_equity_gap_records(fields, combinations)


def subgroup_equity_gap_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    column_combinations: Sequence[Sequence[str]],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
    top_n: int = 30,
) -> SubgroupEquityGapChart:
    """Chart the largest adverse structural differences from dataset averages.

    Review categories are descriptive prioritisation aids, not statistical tests.
    """
    records = subgroup_equity_gap_data(
        splink_dataframe_or_dataframes,
        columns,
        column_combinations,
        auxiliary_columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    return SubgroupEquityGapChart(records[:top_n])
