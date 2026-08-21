import pytest

from splink.exploratory import (
    combination_ambiguity_chart,
    combination_ambiguity_data,
    linkage_column_quality_chart,
    linkage_column_quality_data,
)
from tests.decorator import mark_with_dialects_including


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_pre_linkage_metrics_keep_input_tables_separate(dialect, test_helpers):
    db_api = test_helpers[dialect].db_api()
    left = db_api.register(
        {
            "record_id": [1, 2, 3],
            "name": ["Alice", "Alice", "Bob"],
            "postcode": ["A1", "A1", "B2"],
        }
    )
    right = db_api.register(
        {
            "record_id": [4, 5],
            "name": ["Cara", ""],
            "postcode": ["C3", "D4"],
        }
    )

    quality = linkage_column_quality_data(
        [left, right],
        columns=["name"],
        table_names_for_chart=["Party A", "Party B"],
    )
    assert {record["source_dataset"] for record in quality} == {
        "Party A",
        "Party B",
    }
    left_quality = next(
        record for record in quality if record["source_dataset"] == "Party A"
    )
    right_quality = next(
        record for record in quality if record["source_dataset"] == "Party B"
    )
    assert left_quality["usable_completeness"] == pytest.approx(1.0)
    assert right_quality["usable_completeness"] == pytest.approx(0.5)

    ambiguity = combination_ambiguity_data(
        [left, right],
        column_combinations=[["name", "postcode"]],
        table_names_for_chart=["Party A", "Party B"],
    )
    assert len(ambiguity) == 2
    assert {record["source_dataset"] for record in ambiguity} == {
        "Party A",
        "Party B",
    }


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_pre_linkage_chart_specs_validate_with_altair(dialect, test_helpers):
    db_api = test_helpers[dialect].db_api()
    sdf = db_api.register(
        {
            "record_id": [1, 2, 3],
            "name": ["Alice", "Alice", "Bob"],
            "postcode": ["A1", "A1", "B2"],
        }
    )

    quality = linkage_column_quality_chart(sdf, columns=["name", "postcode"])
    ambiguity = combination_ambiguity_chart(
        sdf,
        column_combinations=[["name", "postcode"], ["postcode"]],
    )

    quality_spec = quality.altair_chart.to_dict()
    ambiguity_spec = ambiguity.altair_chart.to_dict()
    assert quality_spec["title"]["text"] == "Linkage-column quality"
    assert ambiguity_spec["title"]["text"] == "Exact-signature ambiguity"
