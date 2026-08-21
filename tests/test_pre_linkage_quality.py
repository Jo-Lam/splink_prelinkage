import pytest

from splink.exploratory import (
    combination_ambiguity_chart,
    combination_ambiguity_data,
    linkage_column_quality_chart,
    linkage_column_quality_data,
)
from tests.decorator import mark_with_dialects_including


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_linkage_column_quality_is_blank_aware_and_privacy_safe(
    dialect, test_helpers
):
    db_api = test_helpers[dialect].db_api()
    sdf = db_api.register(
        {
            "record_id": [1, 2, 3, 4, 5, 6],
            "first_name": ["Alice", "Alice", "Bob", "", None, "Cara"],
            "surname": ["Smith", "Smith", "Jones", "Brown", "Brown", " "],
        }
    )

    records = linkage_column_quality_data(
        sdf,
        columns=["first_name", "surname"],
        table_names_for_chart=["submitted records"],
    )
    by_column = {record["column_name"]: record for record in records}

    first_name = by_column["first_name"]
    assert first_name["source_dataset"] == "submitted records"
    assert first_name["total_rows"] == 6
    assert first_name["observed_rows"] == 4
    assert first_name["missing_rows"] == 2
    assert first_name["usable_completeness"] == pytest.approx(4 / 6)
    assert first_name["distinct_values"] == 3
    assert first_name["distinct_value_rate"] == pytest.approx(3 / 4)
    assert first_name["singleton_record_share"] == pytest.approx(2 / 4)
    assert first_name["repeated_record_share"] == pytest.approx(2 / 4)
    assert first_name["largest_value_count"] == 2
    assert first_name["largest_value_share"] == pytest.approx(2 / 4)
    assert first_name["top_5pct_values_record_count"] == 2
    assert first_name["top_5pct_values_record_share"] == pytest.approx(2 / 4)

    serialised = repr(records)
    assert "Alice" not in serialised
    assert "Smith" not in serialised

    chart = linkage_column_quality_chart(sdf, columns=["first_name", "surname"])
    assert chart.chart_dict["data"]["values"]


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_combination_ambiguity_describes_exact_signature_groups(
    dialect, test_helpers
):
    db_api = test_helpers[dialect].db_api()
    sdf = db_api.register(
        {
            "record_id": [1, 2, 3, 4, 5, 6],
            "first_name": ["Alice", "Alice", "Bob", "", None, "Cara"],
            "surname": ["Smith", "Smith", "Jones", "Brown", "Brown", " "],
        }
    )

    records = combination_ambiguity_data(
        sdf,
        column_combinations=[
            ["first_name", "surname"],
            ["surname"],
        ],
        table_names_for_chart=["submitted records"],
    )
    full = next(
        record
        for record in records
        if record["column_combination"] == "first_name + surname"
    )

    assert full["total_rows"] == 6
    assert full["complete_rows"] == 3
    assert full["usable_rows"] == 6
    assert full["distinct_signatures"] == 4
    assert full["unique_records"] == 2
    assert full["ambiguous_records"] == 4
    assert full["largest_signature_group"] == 2
    assert full["unique_record_share"] == pytest.approx(2 / 6)
    assert full["ambiguous_record_share"] == pytest.approx(4 / 6)
    assert full["maximum_one_to_one_assignment_share"] == pytest.approx(4 / 6)

    serialised = repr(records)
    assert "Alice" not in serialised
    assert "Smith" not in serialised

    chart = combination_ambiguity_chart(
        sdf,
        column_combinations=[["first_name", "surname"], ["surname"]],
    )
    assert chart.chart_dict["data"]["values"]


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_table_display_names_must_match_inputs(dialect, test_helpers):
    db_api = test_helpers[dialect].db_api()
    left = db_api.register({"record_id": [1], "name": ["A"]})
    right = db_api.register({"record_id": [2], "name": ["B"]})

    with pytest.raises(ValueError, match="one name for each input table"):
        linkage_column_quality_data(
            [left, right],
            columns=["name"],
            table_names_for_chart=["left only"],
        )
