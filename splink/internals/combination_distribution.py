from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart
from splink.internals.input_column import InputColumn
from splink.internals.misc import join_sql_with_union_all
from splink.internals.pipeline import CTEPipeline
from splink.internals.pre_linkage_quality import (
    _apply_table_names,
    _enqueue_concatenated_inputs,
    _normalised_value_sql,
    _sql_literal,
    _validate_table_names,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import (
    get_db_api_from_inputs,
    splink_dataframes_to_dict,
)


class CombinationSizeDistributionChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "combination_size_distribution.json"


def combination_size_distribution_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    column_combinations: Sequence[Sequence[str]],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Return exact-signature group sizes in interpretable cardinality buckets."""
    if not column_combinations:
        raise ValueError("At least one column combination must be provided.")
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    dialect = db_api.sql_dialect.sqlglot_dialect
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    queries: list[str] = []
    for combination_order, raw_combination in enumerate(column_combinations):
        if not raw_combination:
            raise ValueError("Column combinations cannot be empty.")
        combination = [
            InputColumn(column, sqlglot_dialect_str=dialect)
            for column in raw_combination
        ]
        aliases = [f"field_{i}" for i in range(len(combination))]
        values = ", ".join(
            f"{_normalised_value_sql(column.name, blank_as_null)} as {alias}"
            for column, alias in zip(combination, aliases)
        )
        usable = " or ".join(f"{alias} is not null" for alias in aliases)
        grouped = ", ".join(aliases)
        label = " + ".join(column.unquote().name for column in combination)
        queries.append(
            f"""
            select * from (with base as (
                select {source} as source_dataset, {values}
                from __splink__df_pre_linkage_inputs
            ), signatures as (
                select source_dataset, {grouped}, count(*) as signature_size
                from base
                where {usable}
                group by source_dataset, {grouped}
            ), bucketed as (
                select source_dataset, signature_size,
                       case
                           when signature_size <= 4 then cast(signature_size as varchar)
                           when signature_size <= 9 then '5-9'
                           when signature_size <= 19 then '10-19'
                           when signature_size <= 49 then '20-49'
                           when signature_size <= 99 then '50-99'
                           else '100+'
                       end as cardinality_bucket,
                       case
                           when signature_size = 1 then 1
                           when signature_size = 2 then 2
                           when signature_size = 3 then 3
                           when signature_size = 4 then 4
                           when signature_size <= 9 then 5
                           when signature_size <= 19 then 6
                           when signature_size <= 49 then 7
                           when signature_size <= 99 then 8
                           else 9
                       end as bucket_order
                from signatures
            ), distribution as (
                select source_dataset, cardinality_bucket, bucket_order,
                       count(*) as signature_groups,
                       sum(signature_size) as records
                from bucketed
                group by source_dataset, cardinality_bucket, bucket_order
            )
            select source_dataset, {_sql_literal(label)} as column_combination,
                   {combination_order} as combination_order,
                   {len(combination)} as number_of_columns,
                   cardinality_bucket, bucket_order, signature_groups, records,
                   cast(signature_groups * 1.0 / sum(signature_groups) over (
                       partition by source_dataset
                   ) as float) as signature_group_share,
                   cast(records * 1.0 / sum(records) over (
                       partition by source_dataset
                   ) as float) as usable_record_share
            from distribution
            ) as combination_distribution
            """
        )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_combination_size_distribution",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, names)


def combination_size_distribution_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    column_combinations: Sequence[Sequence[str]],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> CombinationSizeDistributionChart:
    """Chart exact-signature group sizes for each PII combination.

    Fixed cardinality buckets show how quickly exact-match ambiguity grows.
    """
    return CombinationSizeDistributionChart(
        combination_size_distribution_data(
            splink_dataframe_or_dataframes,
            column_combinations,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
    )
