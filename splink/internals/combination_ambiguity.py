from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.input_column import InputColumn
from splink.internals.misc import join_sql_with_union_all
from splink.internals.pipeline import CTEPipeline
from splink.internals.pre_linkage_charts import CombinationAmbiguityChart
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


def _combination_query(
    *,
    source_column: str,
    select_values: str,
    complete_condition: str,
    usable_condition: str,
    grouped_columns: str,
    combination_label: str,
    combination_order: int,
    number_of_columns: int,
) -> str:
    return f"""
        select * from (with base as (
            select {source_column} as source_dataset, {select_values}
            from __splink__df_pre_linkage_inputs
        ),
        row_stats as (
            select source_dataset,
                   count(*) as total_rows,
                   sum(case when {complete_condition} then 1 else 0 end)
                       as complete_rows,
                   sum(case when {usable_condition} then 1 else 0 end)
                       as usable_rows
            from base
            group by source_dataset
        ),
        signature_counts as (
            select source_dataset, {grouped_columns}, count(*) as signature_size
            from base
            where {usable_condition}
            group by source_dataset, {grouped_columns}
        ),
        signature_stats as (
            select source_dataset,
                   count(*) as distinct_signatures,
                   sum(case when signature_size = 1 then signature_size else 0 end)
                       as unique_records,
                   sum(case when signature_size > 1 then signature_size else 0 end)
                       as ambiguous_records,
                   max(signature_size) as largest_signature_group
            from signature_counts
            group by source_dataset
        )
        select r.source_dataset,
               {_sql_literal(combination_label)} as column_combination,
               {combination_order} as combination_order,
               {number_of_columns} as number_of_columns,
               r.total_rows,
               r.complete_rows,
               cast(r.complete_rows * 1.0 / nullif(r.total_rows, 0) as float)
                   as complete_record_share,
               r.usable_rows,
               cast(r.usable_rows * 1.0 / nullif(r.total_rows, 0) as float)
                   as usable_record_share,
               cast((r.total_rows - r.usable_rows) * 1.0
                   / nullif(r.total_rows, 0) as float) as unusable_record_share,
               coalesce(s.distinct_signatures, 0) as distinct_signatures,
               coalesce(s.unique_records, 0) as unique_records,
               cast(coalesce(s.unique_records, 0) * 1.0
                   / nullif(r.total_rows, 0) as float) as unique_record_share,
               coalesce(s.ambiguous_records, 0) as ambiguous_records,
               cast(coalesce(s.ambiguous_records, 0) * 1.0
                   / nullif(r.total_rows, 0) as float) as ambiguous_record_share,
               coalesce(s.largest_signature_group, 0) as largest_signature_group,
               cast(coalesce(s.distinct_signatures, 0) * 1.0
                   / nullif(r.total_rows, 0) as float)
                   as maximum_one_to_one_assignment_share
        from row_stats r
        left join signature_stats s on r.source_dataset = s.source_dataset
        ) as combination_quality
    """


def combination_ambiguity_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    column_combinations: Sequence[Sequence[str]],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Assess exact-signature ambiguity without returning identifier values.

    The result describes structural uniqueness within each input table. It is
    not a truth-based estimate of linkage precision or recall.
    """
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    splink_df_dict = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    table_name_lookup = _validate_table_names(
        splink_df_dict, table_names_for_chart
    )
    if not column_combinations:
        raise ValueError("At least one column combination must be provided.")

    dialect = db_api.sql_dialect.sqlglot_dialect
    pipeline = CTEPipeline()
    source_column = _enqueue_concatenated_inputs(pipeline, splink_df_dict)
    queries: list[str] = []

    for combination_order, raw_combination in enumerate(column_combinations):
        if not raw_combination:
            raise ValueError("Column combinations cannot be empty.")
        combination = [
            InputColumn(column, sqlglot_dialect_str=dialect)
            for column in raw_combination
        ]
        aliases = [f"field_{i}" for i in range(len(combination))]
        select_values = ", ".join(
            f"{_normalised_value_sql(column.name, blank_as_null)} as {alias}"
            for column, alias in zip(combination, aliases)
        )
        queries.append(
            _combination_query(
                source_column=source_column,
                select_values=select_values,
                complete_condition=" and ".join(
                    f"{alias} is not null" for alias in aliases
                ),
                usable_condition=" or ".join(
                    f"{alias} is not null" for alias in aliases
                ),
                grouped_columns=", ".join(aliases),
                combination_label=" + ".join(
                    column.unquote().name for column in combination
                ),
                combination_order=combination_order,
                number_of_columns=len(combination),
            )
        )

    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_combination_ambiguity",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, table_name_lookup)


def combination_ambiguity_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    column_combinations: Sequence[Sequence[str]],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> CombinationAmbiguityChart:
    """Chart exact-signature uniqueness across linkage-column combinations."""
    records = combination_ambiguity_data(
        splink_dataframe_or_dataframes,
        column_combinations,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    return CombinationAmbiguityChart(records)
