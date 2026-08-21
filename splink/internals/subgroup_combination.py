from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart
from splink.internals.combination_ambiguity import combination_ambiguity_data
from splink.internals.input_column import InputColumn
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


class SubgroupCombinationAmbiguityChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "subgroup_combination_ambiguity.json"


def subgroup_combination_ambiguity_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    column_combinations: Sequence[Sequence[str]],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Assess exact-signature ambiguity within auxiliary-variable strata."""
    if not column_combinations:
        raise ValueError("At least one column combination must be provided.")
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    first = next(iter(frames.values()))
    auxiliary = _input_columns(auxiliary_columns, first, db_api)
    if not auxiliary:
        raise ValueError("At least one auxiliary column must be provided.")
    dialect = db_api.sql_dialect.sqlglot_dialect
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    queries: list[str] = []

    for auxiliary_order, group_column in enumerate(auxiliary):
        group_value = _normalised_value_sql(group_column.name, blank_as_null)
        group_label = group_column.unquote().name
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
            complete = " and ".join(f"{alias} is not null" for alias in aliases)
            usable = " or ".join(f"{alias} is not null" for alias in aliases)
            grouped = ", ".join(aliases)
            label = " + ".join(column.unquote().name for column in combination)
            queries.append(
                f"""
                select * from (with base as (
                    select {source} as source_dataset,
                           coalesce({group_value}, '[MISSING]')
                               as auxiliary_value,
                           {values}
                    from __splink__df_pre_linkage_inputs
                ), row_stats as (
                    select source_dataset, auxiliary_value,
                           count(*) as total_rows,
                           sum(case when {complete} then 1 else 0 end)
                               as complete_rows,
                           sum(case when {usable} then 1 else 0 end)
                               as usable_rows
                    from base
                    group by source_dataset, auxiliary_value
                ), signature_counts as (
                    select source_dataset, auxiliary_value, {grouped},
                           count(*) as signature_size
                    from base
                    where {usable}
                    group by source_dataset, auxiliary_value, {grouped}
                ), signature_stats as (
                    select source_dataset, auxiliary_value,
                           count(*) as distinct_signatures,
                           sum(case when signature_size = 1
                               then signature_size else 0 end) as unique_records,
                           sum(case when signature_size > 1
                               then signature_size else 0 end) as ambiguous_records,
                           max(signature_size) as largest_signature_group
                    from signature_counts
                    group by source_dataset, auxiliary_value
                )
                select r.source_dataset,
                       {_sql_literal(group_label)} as auxiliary_column,
                       {auxiliary_order} as auxiliary_order,
                       r.auxiliary_value,
                       {_sql_literal(label)} as column_combination,
                       {combination_order} as combination_order,
                       {len(combination)} as number_of_columns,
                       r.total_rows as group_records,
                       r.complete_rows,
                       cast(r.complete_rows * 1.0 / r.total_rows as float)
                           as complete_record_share,
                       r.usable_rows,
                       cast((r.total_rows - r.usable_rows) * 1.0
                           / r.total_rows as float) as unusable_record_share,
                       coalesce(s.distinct_signatures, 0) as distinct_signatures,
                       coalesce(s.unique_records, 0) as unique_records,
                       cast(coalesce(s.unique_records, 0) * 1.0
                           / r.total_rows as float) as unique_record_share,
                       coalesce(s.ambiguous_records, 0) as ambiguous_records,
                       cast(coalesce(s.ambiguous_records, 0) * 1.0
                           / r.total_rows as float) as ambiguous_record_share,
                       coalesce(s.largest_signature_group, 0)
                           as largest_signature_group,
                       cast(coalesce(s.distinct_signatures, 0) * 1.0
                           / r.total_rows as float)
                           as maximum_one_to_one_assignment_share
                from row_stats r
                left join signature_stats s
                  on r.source_dataset = s.source_dataset
                 and r.auxiliary_value = s.auxiliary_value
                ) as subgroup_combination_ambiguity
                """
            )

    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_subgroup_combination_ambiguity",
    )
    records = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    records = _apply_table_names(records, names)
    overall = combination_ambiguity_data(
        splink_dataframe_or_dataframes,
        column_combinations,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    overall_index = {
        (row["source_dataset"], row["column_combination"]): row for row in overall
    }
    gap_metrics = (
        "complete_record_share",
        "unique_record_share",
        "ambiguous_record_share",
        "unusable_record_share",
        "maximum_one_to_one_assignment_share",
    )
    for row in records:
        baseline = overall_index[(
            row["source_dataset"],
            row["column_combination"],
        )]
        row["group_record_share"] = row["group_records"] / baseline["total_rows"]
        for metric in gap_metrics:
            row[f"{metric}_gap"] = float(row[metric]) - float(baseline[metric])
    return records


def subgroup_combination_ambiguity_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    column_combinations: Sequence[Sequence[str]],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> SubgroupCombinationAmbiguityChart:
    """Chart exact-signature ambiguity within auxiliary-variable groups.

    This is a structural diagnostic, not a test of realised linkage fairness.
    """
    return SubgroupCombinationAmbiguityChart(
        subgroup_combination_ambiguity_data(
            splink_dataframe_or_dataframes,
            column_combinations,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
    )
