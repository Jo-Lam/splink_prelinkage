from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.misc import join_sql_with_union_all
from splink.internals.pipeline import CTEPipeline
from splink.internals.pre_linkage_charts import LinkageColumnQualityChart
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import (
    get_db_api_from_inputs,
    splink_dataframes_to_dict,
)
from splink.internals.vertically_concatenate import vertically_concatenate_sql


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_table_names(
    splink_df_dict: dict[str, SplinkDataFrame],
    table_names_for_chart: Sequence[str] | None,
) -> dict[str, str]:
    physical_names = [df.physical_name for df in splink_df_dict.values()]
    if table_names_for_chart is None:
        display_names = [f"input_data_{i + 1}" for i in range(len(physical_names))]
    else:
        if len(table_names_for_chart) != len(physical_names):
            raise ValueError(
                "table_names_for_chart must have one name for each input table."
            )
        display_names = list(table_names_for_chart)
    return dict(zip(physical_names, display_names))


def _enqueue_concatenated_inputs(
    pipeline: CTEPipeline,
    splink_df_dict: dict[str, SplinkDataFrame],
) -> str:
    pipeline.enqueue_sql(
        vertically_concatenate_sql(
            splink_df_dict,
            source_dataset_input_column=None,
        ),
        "__splink__df_concat",
    )
    source_column = "__pre_linkage_source_dataset"
    first_df = next(iter(splink_df_dict.values()))
    source_expression = (
        _sql_literal(first_df.physical_name)
        if len(splink_df_dict) == 1
        else "source_dataset"
    )
    pipeline.enqueue_sql(
        f"""
        select {source_expression} as {source_column}, *
        from __splink__df_concat
        """,
        "__splink__df_pre_linkage_inputs",
    )
    return source_column


def _normalised_value_sql(quoted_column: str, blank_as_null: bool) -> str:
    if blank_as_null:
        return f"nullif(trim(cast({quoted_column} as varchar)), '')"
    return quoted_column


def _input_columns(
    columns: Sequence[str] | None,
    first_df: SplinkDataFrame,
    db_api: DatabaseAPISubClass,
) -> list[InputColumn]:
    if columns is None:
        return list(first_df.columns)
    dialect = db_api.sql_dialect.sqlglot_dialect
    return [InputColumn(column, sqlglot_dialect_str=dialect) for column in columns]


def _apply_table_names(
    records: list[dict[str, Any]], table_name_lookup: dict[str, str]
) -> list[dict[str, Any]]:
    for record in records:
        physical_name = record["source_dataset"]
        record["source_dataset"] = table_name_lookup.get(
            physical_name, physical_name
        )
    return records


def linkage_column_quality_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str] | None = None,
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Calculate privacy-safe quality measures for candidate linkage columns.

    Unlike :func:`completeness_chart`, ``blank_as_null=True`` treats empty and
    whitespace-only strings as unavailable. No observed values are returned.

    Args:
        splink_dataframe_or_dataframes: One or more registered Splink dataframes.
        columns: Column names to assess. All columns are used when omitted.
        table_names_for_chart: Optional display names for the input tables.
        blank_as_null: Treat empty and whitespace-only strings as missing.

    Returns:
        Aggregate records containing usable completeness, distinctness,
        singleton and repeated-record shares, and largest-value concentration.
    """
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    splink_df_dict = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    table_name_lookup = _validate_table_names(
        splink_df_dict, table_names_for_chart
    )
    first_df = next(iter(splink_df_dict.values()))
    input_columns = _input_columns(columns, first_df, db_api)
    if not input_columns:
        return []

    pipeline = CTEPipeline()
    source_column = _enqueue_concatenated_inputs(pipeline, splink_df_dict)
    queries: list[str] = []

    for column_order, column in enumerate(input_columns):
        quoted_column = column.name
        column_label = column.unquote().name
        value_sql = _normalised_value_sql(quoted_column, blank_as_null)
        queries.append(
            f"""
            select * from (with values_ as (
                select {source_column} as source_dataset,
                       {value_sql} as value
                from __splink__df_pre_linkage_inputs
            ),
            source_stats as (
                select source_dataset,
                       count(*) as total_rows,
                       count(value) as observed_rows
                from values_
                group by source_dataset
            ),
            value_counts as (
                select source_dataset, value, count(*) as value_count
                from values_
                where value is not null
                group by source_dataset, value
            ),
            ranked_value_counts as (
                select source_dataset, value, value_count,
                       row_number() over (
                           partition by source_dataset
                           order by value_count desc, value
                       ) as value_rank,
                       count(*) over (partition by source_dataset)
                           as distinct_value_count
                from value_counts
            ),
            value_stats as (
                select source_dataset,
                       count(*) as distinct_values,
                       sum(case when value_count = 1 then value_count else 0 end)
                           as singleton_records,
                       sum(case when value_count > 1 then value_count else 0 end)
                           as repeated_records,
                       max(value_count) as largest_value_count,
                       sum(case when value_rank <= greatest(
                               1, ceil(distinct_value_count * 0.05)
                           ) then value_count else 0 end)
                           as top_5pct_values_record_count
                from ranked_value_counts
                group by source_dataset
            )
            select s.source_dataset,
                   {_sql_literal(column_label)} as column_name,
                   {column_order} as column_order,
                   s.total_rows,
                   s.observed_rows,
                   s.total_rows - s.observed_rows as missing_rows,
                   cast(s.observed_rows * 1.0 / nullif(s.total_rows, 0) as float)
                       as usable_completeness,
                   coalesce(v.distinct_values, 0) as distinct_values,
                   cast(coalesce(v.distinct_values, 0) * 1.0
                       / nullif(s.observed_rows, 0) as float) as distinct_value_rate,
                   cast(coalesce(v.singleton_records, 0) * 1.0
                       / nullif(s.observed_rows, 0) as float) as singleton_record_share,
                   cast(coalesce(v.repeated_records, 0) * 1.0
                       / nullif(s.observed_rows, 0) as float) as repeated_record_share,
                   coalesce(v.largest_value_count, 0) as largest_value_count,
                   cast(coalesce(v.largest_value_count, 0) * 1.0
                       / nullif(s.observed_rows, 0) as float) as largest_value_share
                   ,coalesce(v.top_5pct_values_record_count, 0)
                       as top_5pct_values_record_count
                   ,cast(coalesce(v.top_5pct_values_record_count, 0) * 1.0
                       / nullif(s.observed_rows, 0) as float)
                       as top_5pct_values_record_share
            from source_stats s
            left join value_stats v
                on s.source_dataset = v.source_dataset
            ) as column_quality
            """
        )

    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_linkage_column_quality",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, table_name_lookup)


def linkage_column_quality_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str] | None = None,
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> LinkageColumnQualityChart:
    """Chart aggregate quality measures for candidate linkage columns.

    The chart does not include observed identifier values. It is suitable for
    initial data-quality assessment, but does not estimate realised linkage
    precision or recall.
    """
    records = linkage_column_quality_data(
        splink_dataframe_or_dataframes,
        columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    return LinkageColumnQualityChart(records)
