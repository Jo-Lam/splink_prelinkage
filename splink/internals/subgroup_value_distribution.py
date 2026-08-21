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


class SubgroupValueConcentrationChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "subgroup_value_concentration.json"


class SubgroupValueDominanceDistributionChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "subgroup_value_dominance_distribution.json"


def _inputs(
    splink_dataframe_or_dataframes,
    columns,
    auxiliary_columns,
    table_names_for_chart,
):
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    first = next(iter(frames.values()))
    values = _input_columns(columns, first, db_api)
    auxiliary = _input_columns(auxiliary_columns, first, db_api)
    if not values:
        raise ValueError("At least one linkage column must be provided.")
    if not auxiliary:
        raise ValueError("At least one auxiliary column must be provided.")
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    return db_api, names, values, auxiliary, pipeline, source


def subgroup_value_concentration_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
    maximum_points: int = 200,
) -> list[dict[str, Any]]:
    """Return cumulative value concentration within auxiliary-variable strata."""
    if maximum_points < 2:
        raise ValueError("maximum_points must be at least 2.")
    db_api, names, values, auxiliary, pipeline, source = _inputs(
        splink_dataframe_or_dataframes,
        columns,
        auxiliary_columns,
        table_names_for_chart,
    )
    queries: list[str] = []
    for auxiliary_order, auxiliary_column in enumerate(auxiliary):
        auxiliary_value = _normalised_value_sql(
            auxiliary_column.name, blank_as_null
        )
        auxiliary_label = auxiliary_column.unquote().name
        for column_order, value_column in enumerate(values):
            value = _normalised_value_sql(value_column.name, blank_as_null)
            value_label = value_column.unquote().name
            queries.append(
                f"""
                select * from (with counts as (
                    select {source} as source_dataset,
                           coalesce({auxiliary_value}, '[MISSING]')
                               as auxiliary_value,
                           {value} as value, count(*) as frequency
                    from __splink__df_pre_linkage_inputs
                    where {value} is not null
                    group by {source}, auxiliary_value, {value}
                ), ranked as (
                    select source_dataset, auxiliary_value, frequency,
                           row_number() over (
                               partition by source_dataset, auxiliary_value
                               order by frequency desc, value
                           ) as value_rank,
                           sum(frequency) over (
                               partition by source_dataset, auxiliary_value
                               order by frequency desc, value
                               rows unbounded preceding
                           ) as cumulative_records,
                           sum(frequency) over (
                               partition by source_dataset, auxiliary_value
                           ) as observed_rows,
                           count(*) over (
                               partition by source_dataset, auxiliary_value
                           ) as distinct_values
                    from counts
                )
                select source_dataset,
                       {_sql_literal(auxiliary_label)} as auxiliary_column,
                       {auxiliary_order} as auxiliary_order,
                       auxiliary_value,
                       {_sql_literal(value_label)} as column_name,
                       {column_order} as column_order,
                       value_rank, frequency,
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
                       greatest(
                           1,
                           ceil(distinct_values * 1.0 / {maximum_points})
                       )
                   ) = 0
                ) as subgroup_value_concentration
                """
            )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_subgroup_value_concentration",
    )
    records = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(records, names)


def subgroup_value_concentration_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
    maximum_points: int = 200,
) -> SubgroupValueConcentrationChart:
    """Chart cumulative PII value concentration within auxiliary groups.

    The chart reveals distributional differences without returning PII values.
    """
    return SubgroupValueConcentrationChart(
        subgroup_value_concentration_data(
            splink_dataframe_or_dataframes,
            columns,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
            maximum_points=maximum_points,
        )
    )


def subgroup_value_dominance_distribution_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Return complete value-frequency distributions within auxiliary strata."""
    db_api, names, values, auxiliary, pipeline, source = _inputs(
        splink_dataframe_or_dataframes,
        columns,
        auxiliary_columns,
        table_names_for_chart,
    )
    queries: list[str] = []
    for auxiliary_order, auxiliary_column in enumerate(auxiliary):
        auxiliary_value = _normalised_value_sql(
            auxiliary_column.name, blank_as_null
        )
        auxiliary_label = auxiliary_column.unquote().name
        for column_order, value_column in enumerate(values):
            value = _normalised_value_sql(value_column.name, blank_as_null)
            value_label = value_column.unquote().name
            queries.append(
                f"""
                select * from (with counts as (
                    select {source} as source_dataset,
                           coalesce({auxiliary_value}, '[MISSING]')
                               as auxiliary_value,
                           {value} as value, count(*) as frequency
                    from __splink__df_pre_linkage_inputs
                    where {value} is not null
                    group by {source}, auxiliary_value, {value}
                ), distribution as (
                    select source_dataset, auxiliary_value, frequency,
                           count(*) as values_at_frequency,
                           sum(frequency) as records_at_frequency
                    from counts
                    group by source_dataset, auxiliary_value, frequency
                )
                select source_dataset,
                       {_sql_literal(auxiliary_label)} as auxiliary_column,
                       {auxiliary_order} as auxiliary_order,
                       auxiliary_value,
                       {_sql_literal(value_label)} as column_name,
                       {column_order} as column_order,
                       frequency, values_at_frequency, records_at_frequency,
                       cast(values_at_frequency * 1.0
                           / sum(values_at_frequency) over (
                               partition by source_dataset, auxiliary_value
                           ) as float) as distinct_value_share,
                       cast(records_at_frequency * 1.0
                           / sum(records_at_frequency) over (
                               partition by source_dataset, auxiliary_value
                           ) as float) as record_share
                from distribution
                ) as subgroup_value_dominance
                """
            )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_subgroup_value_dominance_distribution",
    )
    records = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(records, names)


def subgroup_value_dominance_distribution_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> SubgroupValueDominanceDistributionChart:
    """Chart complete PII value-frequency distributions within groups."""
    return SubgroupValueDominanceDistributionChart(
        subgroup_value_dominance_distribution_data(
            splink_dataframe_or_dataframes,
            columns,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
    )
