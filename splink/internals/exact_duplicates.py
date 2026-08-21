from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart
from splink.internals.pre_linkage_quality import (
    _apply_table_names,
    _enqueue_concatenated_inputs,
    _input_columns,
    _sql_literal,
    _validate_table_names,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import (
    get_db_api_from_inputs,
    splink_dataframes_to_dict,
)


class ExactDuplicateChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "exact_duplicates.json"


def exact_duplicate_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Assess rows that are exactly equal across the submitted columns.

    Values are grouped as submitted. No case conversion, trimming or other
    normalisation is applied, and no observed values are returned.
    """
    if not columns:
        raise ValueError("At least one duplicate-defining column must be provided.")
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    parsed = _input_columns(columns, next(iter(frames.values())), db_api)
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    grouped = ", ".join(column.name for column in parsed)
    label = " + ".join(column.unquote().name for column in parsed)
    pipeline.enqueue_sql(
        f"""
        select * from (with row_stats as (
            select {source} as source_dataset, count(*) as total_rows
            from __splink__df_pre_linkage_inputs
            group by {source}
        ), duplicate_groups as (
            select {source} as source_dataset, {grouped},
                   count(*) as group_size
            from __splink__df_pre_linkage_inputs
            group by {source}, {grouped}
        ), duplicate_stats as (
            select source_dataset,
                   sum(case when group_size > 1 then 1 else 0 end)
                       as duplicate_groups,
                   sum(case when group_size > 1 then group_size else 0 end)
                       as records_in_duplicate_groups,
                   sum(case when group_size > 1 then group_size - 1 else 0 end)
                       as excess_duplicate_rows,
                   max(group_size) as largest_duplicate_group
            from duplicate_groups
            group by source_dataset
        )
        select r.source_dataset,
               {_sql_literal(label)} as columns_used,
               r.total_rows,
               coalesce(d.duplicate_groups, 0) as duplicate_groups,
               coalesce(d.records_in_duplicate_groups, 0)
                   as records_in_duplicate_groups,
               cast(coalesce(d.records_in_duplicate_groups, 0) * 1.0
                   / nullif(r.total_rows, 0) as float) as duplicate_record_share,
               coalesce(d.excess_duplicate_rows, 0) as excess_duplicate_rows,
               coalesce(d.largest_duplicate_group, 1) as largest_duplicate_group
        from row_stats r
        left join duplicate_stats d on r.source_dataset = d.source_dataset
        ) as exact_duplicates
        """,
        "__splink__df_exact_duplicates",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, names)


def exact_duplicate_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
) -> ExactDuplicateChart:
    """Chart exact duplicate rows across the declared columns.

    Repeated rows may be valid events or ingestion duplicates; this does not decide.
    """
    return ExactDuplicateChart(
        exact_duplicate_data(
            splink_dataframe_or_dataframes,
            columns,
            table_names_for_chart,
        )
    )
