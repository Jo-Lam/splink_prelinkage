from __future__ import annotations

from collections import defaultdict
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


class MissingnessByColumnChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "missingness_by_column.json"


class MissingnessPatternChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "missingness_patterns.json"


class SubgroupMissingnessChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "subgroup_missingness.json"


def missingness_by_column_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Return missingness counts and shares for each linkage column."""
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    parsed = _input_columns(columns, next(iter(frames.values())), db_api)
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    queries: list[str] = []
    for column_order, column in enumerate(parsed):
        value = _normalised_value_sql(column.name, blank_as_null)
        label = column.unquote().name
        queries.append(
            f"""
            select {source} as source_dataset,
                   {_sql_literal(label)} as column_name,
                   {column_order} as column_order,
                   count(*) as total_rows,
                   sum(case when {value} is null then 1 else 0 end)
                       as missing_rows,
                   cast(sum(case when {value} is null then 1 else 0 end) * 1.0
                       / nullif(count(*), 0) as float) as missing_record_share
            from __splink__df_pre_linkage_inputs
            group by {source}
            """
        )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_missingness_by_column",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, names)


def missingness_by_column_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> MissingnessByColumnChart:
    """Chart the missing-record share for each declared linkage column."""
    return MissingnessByColumnChart(
        missingness_by_column_data(
            splink_dataframe_or_dataframes,
            columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
    )


def _pattern_expressions(parsed_columns, aliases):
    missing_parts = ", ".join(
        f"case when {alias} is null then {_sql_literal(column.unquote().name)} end"
        for column, alias in zip(parsed_columns, aliases)
    )
    present_parts = ", ".join(
        f"case when {alias} is not null then {_sql_literal(column.unquote().name)} end"
        for column, alias in zip(parsed_columns, aliases)
    )
    missing_count = " + ".join(
        f"case when {alias} is null then 1 else 0 end" for alias in aliases
    )
    return missing_parts, present_parts, missing_count


def missingness_pattern_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Return all joint missingness patterns, most common first."""
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    parsed = _input_columns(columns, next(iter(frames.values())), db_api)
    if not parsed:
        return []
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    aliases = [f"field_{i}" for i in range(len(parsed))]
    select_values = ", ".join(
        f"{_normalised_value_sql(column.name, blank_as_null)} as {alias}"
        for column, alias in zip(parsed, aliases)
    )
    missing_parts, present_parts, missing_count = _pattern_expressions(
        parsed, aliases
    )
    pipeline.enqueue_sql(
        f"""
        select * from (with base as (
            select {source} as source_dataset, {select_values}
            from __splink__df_pre_linkage_inputs
        ), patterns as (
            select source_dataset,
                   coalesce(nullif(concat_ws(' + ', {missing_parts}), ''), '[none]')
                       as missing_fields,
                   coalesce(nullif(concat_ws(' + ', {present_parts}), ''), '[none]')
                       as present_fields,
                   {missing_count} as missing_field_count
            from base
        ), grouped as (
            select source_dataset, missing_fields, present_fields,
                   missing_field_count, count(*) as records
            from patterns
            group by source_dataset, missing_fields, present_fields,
                     missing_field_count
        )
        select *,
               cast(records * 1.0 / sum(records) over (
                   partition by source_dataset
               ) as float) as record_share
        from grouped
        order by source_dataset, records desc, missing_field_count, missing_fields
        ) as missingness_patterns
        """,
        "__splink__df_missingness_patterns",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    return _apply_table_names(result, names)


def missingness_by_count_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Aggregate rows by the number of missing linkage columns.

    Use this to quantify how much identifier evidence remains on each row.
    """
    patterns = missingness_pattern_data(
        splink_dataframe_or_dataframes,
        columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    totals: dict[tuple[str, int], dict[str, Any]] = {}
    for row in patterns:
        key = (row["source_dataset"], int(row["missing_field_count"]))
        output = totals.setdefault(
            key,
            {
                "source_dataset": key[0],
                "missing_field_count": key[1],
                "number_of_columns": len(columns),
                "records": 0,
                "record_share": 0.0,
            },
        )
        output["records"] += int(row["records"])
        output["record_share"] += float(row["record_share"])
    return sorted(
        totals.values(),
        key=lambda row: (row["source_dataset"], row["missing_field_count"]),
    )


def missingness_pattern_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
    top_n: int = 12,
) -> MissingnessPatternChart:
    """Chart the most frequent joint missingness patterns in each input.

    Patterns describe co-missing fields but do not identify their cause.
    """
    records = missingness_pattern_data(
        splink_dataframe_or_dataframes,
        columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    selected: list[dict[str, Any]] = []
    counts: defaultdict[str, int] = defaultdict(int)
    for row in records:
        dataset = row["source_dataset"]
        if counts[dataset] < top_n:
            selected.append(row)
            counts[dataset] += 1
    return MissingnessPatternChart(selected)


def subgroup_missingness_pattern_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Return joint missingness patterns stratified by auxiliary values."""
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    frames = splink_dataframes_to_dict(splink_dataframe_or_dataframes)
    names = _validate_table_names(frames, table_names_for_chart)
    first = next(iter(frames.values()))
    parsed = _input_columns(columns, first, db_api)
    auxiliary = _input_columns(auxiliary_columns, first, db_api)
    if not parsed or not auxiliary:
        return []
    pipeline = CTEPipeline()
    source = _enqueue_concatenated_inputs(pipeline, frames)
    aliases = [f"field_{i}" for i in range(len(parsed))]
    select_values = ", ".join(
        f"{_normalised_value_sql(column.name, blank_as_null)} as {alias}"
        for column, alias in zip(parsed, aliases)
    )
    missing_parts, present_parts, missing_count = _pattern_expressions(
        parsed, aliases
    )
    queries: list[str] = []
    for group_column in auxiliary:
        group_value = _normalised_value_sql(group_column.name, blank_as_null)
        group_label = group_column.unquote().name
        queries.append(
            f"""
            select * from (with base as (
                select {source} as source_dataset,
                       coalesce({group_value}, '[MISSING]') as auxiliary_value,
                       {select_values}
                from __splink__df_pre_linkage_inputs
            ), patterns as (
                select source_dataset, auxiliary_value,
                       coalesce(nullif(concat_ws(' + ', {missing_parts}), ''), '[none]')
                           as missing_fields,
                       coalesce(nullif(concat_ws(' + ', {present_parts}), ''), '[none]')
                           as present_fields,
                       {missing_count} as missing_field_count
                from base
            ), grouped as (
                select source_dataset, auxiliary_value, missing_fields,
                       present_fields, missing_field_count, count(*) as records
                from patterns
                group by source_dataset, auxiliary_value, missing_fields,
                         present_fields, missing_field_count
            )
            select source_dataset,
                   {_sql_literal(group_label)} as auxiliary_column,
                   auxiliary_value, missing_fields, present_fields,
                   missing_field_count, records,
                   records as grouped_records
            from grouped
            ) as subgroup_missingness
            """
        )
    pipeline.enqueue_sql(
        join_sql_with_union_all(queries),
        "__splink__df_subgroup_missingness_patterns",
    )
    result = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()
    result = _apply_table_names(result, names)
    totals: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    for row in result:
        key = (
            row["source_dataset"],
            row["auxiliary_column"],
            row["auxiliary_value"],
        )
        totals[key] += int(row["grouped_records"])
    for row in result:
        key = (
            row["source_dataset"],
            row["auxiliary_column"],
            row["auxiliary_value"],
        )
        records = int(row.pop("grouped_records"))
        group_records = totals[key]
        row["records"] = records
        row["group_records"] = group_records
        row["pattern_share_within_group"] = (
            records / group_records if group_records else 0.0
        )
    return sorted(
        result,
        key=lambda row: (
            row["source_dataset"],
            row["auxiliary_column"],
            row["auxiliary_value"],
            -row["records"],
        ),
    )


def subgroup_missingness_summary_data(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> list[dict[str, Any]]:
    """Summarise incomplete rows and their leading pattern by subgroup.

    Auxiliary values are labels, so disclosure review may be required.
    """
    patterns = subgroup_missingness_pattern_data(
        splink_dataframe_or_dataframes,
        columns,
        auxiliary_columns,
        table_names_for_chart,
        blank_as_null=blank_as_null,
    )
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in patterns:
        grouped[
            (
                row["source_dataset"],
                row["auxiliary_column"],
                row["auxiliary_value"],
            )
        ].append(row)
    output: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        group_records = int(rows[0]["group_records"])
        incomplete = [row for row in rows if int(row["missing_field_count"]) > 0]
        any_missing = sum(int(row["records"]) for row in incomplete)
        most_common = max(incomplete, key=lambda row: int(row["records"]), default=None)
        output.append(
            {
                "source_dataset": key[0],
                "auxiliary_column": key[1],
                "auxiliary_value": key[2],
                "group_records": group_records,
                "any_missing_records": any_missing,
                "any_missing_share": (
                    any_missing / group_records if group_records else 0.0
                ),
                "most_common_incomplete_pattern": (
                    most_common["missing_fields"] if most_common else "[none]"
                ),
                "most_common_incomplete_pattern_share": (
                    int(most_common["records"]) / group_records
                    if most_common and group_records
                    else 0.0
                ),
            }
        )
    return sorted(
        output,
        key=lambda row: (
            row["source_dataset"],
            row["auxiliary_column"],
            -row["group_records"],
        ),
    )


def subgroup_missingness_chart(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    columns: Sequence[str],
    auxiliary_columns: Sequence[str],
    table_names_for_chart: Sequence[str] | None = None,
    *,
    blank_as_null: bool = True,
) -> SubgroupMissingnessChart:
    """Chart any-PII missingness across declared auxiliary-variable groups."""
    return SubgroupMissingnessChart(
        subgroup_missingness_summary_data(
            splink_dataframe_or_dataframes,
            columns,
            auxiliary_columns,
            table_names_for_chart,
            blank_as_null=blank_as_null,
        )
    )
