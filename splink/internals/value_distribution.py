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
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import (
    get_db_api_from_inputs,
    splink_dataframes_to_dict,
)


class ValueConcentrationChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "value_concentration.json"


class ValueDominanceDistributionChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "value_dominance_distribution.json"


def value_concentration_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
    maximum_points: int = 500,
) -> list[dict[str, Any]]:
    """Return privacy-safe cumulative concentration curves for linkage columns."""
    if maximum_points < 2:
        raise ValueError("maximum_points must be at least 2.")
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    input_columns = _input_columns(columns, next(iter(frames.values())), db_api)
    pipeline = CTEPipeline()
    source_column = _enqueue_concatenated_inputs(pipeline, frames)
    queries: list[str] = []
    for column_order, column in enumerate(input_columns):
        value_sql = _normalised_value_sql(column.name, blank_as_null)
        label = column.unquote().name
        queries.append(
            f"""
            select * from (with counts as (
                select {source_column} as source_dataset, {value_sql} as value,
                       count(*) as frequency
                from __splink__df_pre_linkage_inputs
                where {value_sql} is not null
                group by {source_column}, {value_sql}
            ), ranked as (
                select source_dataset, frequency,
                       row_number() over (
                           partition by source_dataset
                           order by frequency desc, value
                       ) as value_rank,
                       sum(frequency) over (
                           partition by source_dataset
                           order by frequency desc, value rows unbounded preceding
                       ) as cumulative_records,
                       sum(frequency) over (partition by source_dataset)
                           as observed_rows,
                       count(*) over (partition by source_dataset)
                           as distinct_values
                from counts
            )
            select source_dataset, {_sql_literal(label)} as column_name,
                   {column_order} as column_order,
                   value_rank, frequency,
                   cast(frequency * 1.0 / observed_rows as float)
                       as value_record_share,
                   cast(cumulative_records * 1.0 / observed_rows as float)
                       as cumulative_record_share,
                   cast(value_rank * 1.0 / distinct_values as float)
                       as cumulative_distinct_value_share,
                   observed_rows, distinct_values
            from ranked
            where value_rank = 1
               or value_rank = distinct_values
               or mod(
                   value_rank - 1,
                   greatest(1, ceil(distinct_values * 1.0 / {maximum_points}))
               ) = 0
            ) as value_concentration
            """
        )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_value_concentration",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, names)


def value_concentration_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
    maximum_points: int = 500,
) -> ValueConcentrationChart:
    """Chart cumulative concentration from most to least frequent PII values.

    Curves show concentration shape without returning observed values.
    """
    records = value_concentration_data(
        splink_dataframe_or_dataframes,
        columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
        maximum_points=maximum_points,
    )
    return ValueConcentrationChart(records)


def value_dominance_distribution_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Return the complete frequency distribution without observed values."""
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    input_columns = _input_columns(columns, next(iter(frames.values())), db_api)
    pipeline = CTEPipeline()
    source_column = _enqueue_concatenated_inputs(pipeline, frames)
    queries: list[str] = []
    for column_order, column in enumerate(input_columns):
        value_sql = _normalised_value_sql(column.name, blank_as_null)
        label = column.unquote().name
        queries.append(
            f"""
            select * from (with counts as (
                select {source_column} as source_dataset, {value_sql} as value,
                       count(*) as frequency
                from __splink__df_pre_linkage_inputs
                where {value_sql} is not null
                group by {source_column}, {value_sql}
            ), distribution as (
                select source_dataset, frequency,
                       count(*) as values_at_frequency,
                       sum(frequency) as records_at_frequency
                from counts
                group by source_dataset, frequency
            )
            select source_dataset, {_sql_literal(label)} as column_name,
                   {column_order} as column_order,
                   frequency, values_at_frequency, records_at_frequency,
                   cast(values_at_frequency * 1.0
                       / sum(values_at_frequency) over (
                           partition by source_dataset
                       ) as float) as distinct_value_share,
                   cast(records_at_frequency * 1.0
                       / sum(records_at_frequency) over (
                           partition by source_dataset
                       ) as float) as record_share
            from distribution
            ) as dominance_distribution
            """
        )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_value_dominance_distribution",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, names)


def value_dominance_distribution_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> ValueDominanceDistributionChart:
    """Chart the complete frequency distribution of each PII column.

    This separates one dominant value from many moderately repeated values.
    """
    records = value_dominance_distribution_data(
        splink_dataframe_or_dataframes,
        columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    return ValueDominanceDistributionChart(records)
