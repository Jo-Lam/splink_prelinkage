import pytest

from splink.exploratory import (
    build_pre_linkage_profile,
    combination_size_distribution_chart,
    combination_size_distribution_data,
    compare_pre_linkage_profiles,
    dataset_linkage_overview_chart,
    dataset_linkage_overview_data,
    exact_duplicate_chart,
    exact_duplicate_data,
    missingness_by_column_chart,
    missingness_by_column_data,
    missingness_by_count_data,
    missingness_pattern_chart,
    missingness_pattern_data,
    pre_linkage_reference_comparison_chart,
    reference_value_concentration_chart,
    reference_value_dominance_distribution_chart,
    subgroup_combination_ambiguity_chart,
    subgroup_combination_ambiguity_data,
    subgroup_equity_gap_chart,
    subgroup_equity_gap_data,
    subgroup_linkage_column_quality_chart,
    subgroup_linkage_column_quality_data,
    subgroup_missingness_chart,
    subgroup_missingness_pattern_data,
    subgroup_missingness_summary_data,
    subgroup_value_concentration_chart,
    subgroup_value_concentration_data,
    subgroup_value_dominance_distribution_chart,
    subgroup_value_dominance_distribution_data,
    value_concentration_chart,
    value_concentration_data,
    value_dominance_distribution_chart,
    value_dominance_distribution_data,
)
from tests.decorator import mark_with_dialects_including


def _fixture(db_api):
    return db_api.register(
        {
            "record_id": list(range(1, 9)),
            "first_name": ["A", "A", "B", None, "", "C", "C", "D"],
            "surname": ["X", "X", "Y", "Y", None, "Z", "Z", "W"],
            "sex": ["F", "F", "M", "M", "M", "F", "F", "M"],
        }
    )


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_value_distributions_are_complete_without_raw_values(dialect, test_helpers):
    sdf = _fixture(test_helpers[dialect].db_api())
    concentration = value_concentration_data(sdf, ["first_name"])
    dominance = value_dominance_distribution_data(sdf, ["first_name"])

    assert concentration[-1]["cumulative_record_share"] == pytest.approx(1.0)
    assert concentration[-1]["cumulative_distinct_value_share"] == pytest.approx(1.0)
    by_frequency = {int(row["frequency"]): row for row in dominance}
    assert by_frequency[1]["values_at_frequency"] == 2
    assert by_frequency[1]["records_at_frequency"] == 2
    assert by_frequency[2]["values_at_frequency"] == 2
    assert by_frequency[2]["records_at_frequency"] == 4
    assert "A" not in repr(concentration)
    assert "C" not in repr(dominance)
    assert concentration
    assert dominance

    subgroup_concentration = subgroup_value_concentration_data(
        sdf, ["first_name"], ["sex"]
    )
    subgroup_dominance = subgroup_value_dominance_distribution_data(
        sdf, ["first_name"], ["sex"]
    )
    female_curve = [
        row for row in subgroup_concentration if row["auxiliary_value"] == "F"
    ]
    assert female_curve[-1]["cumulative_record_share"] == pytest.approx(1.0)
    assert {row["auxiliary_value"] for row in subgroup_dominance} == {"F", "M"}
    assert "A" not in repr(subgroup_concentration)


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_missingness_patterns_and_auxiliary_stratification(dialect, test_helpers):
    sdf = _fixture(test_helpers[dialect].db_api())
    by_column = missingness_by_column_data(sdf, ["first_name", "surname"])
    patterns = missingness_pattern_data(sdf, ["first_name", "surname"])
    by_count = missingness_by_count_data(sdf, ["first_name", "surname"])
    subgroup_patterns = subgroup_missingness_pattern_data(
        sdf, ["first_name", "surname"], ["sex"]
    )
    subgroup_summary = subgroup_missingness_summary_data(
        sdf, ["first_name", "surname"], ["sex"]
    )

    missing = {row["column_name"]: row for row in by_column}
    assert missing["first_name"]["missing_record_share"] == pytest.approx(2 / 8)
    assert missing["surname"]["missing_record_share"] == pytest.approx(1 / 8)
    pattern_counts = {row["missing_fields"]: row["records"] for row in patterns}
    assert pattern_counts == {"[none]": 6, "first_name": 1, "first_name + surname": 1}
    count_records = {
        int(row["missing_field_count"]): row["records"] for row in by_count
    }
    assert count_records == {0: 6, 1: 1, 2: 1}
    assert subgroup_patterns
    summaries = {row["auxiliary_value"]: row for row in subgroup_summary}
    assert summaries["F"]["any_missing_share"] == pytest.approx(0.0)
    assert summaries["M"]["any_missing_share"] == pytest.approx(0.5)

    subgroup_fields = subgroup_linkage_column_quality_data(
        sdf, ["first_name", "surname"], ["sex"]
    )
    subgroup_combinations = subgroup_combination_ambiguity_data(
        sdf, [["first_name", "surname"]], ["sex"]
    )
    equity = subgroup_equity_gap_data(
        sdf,
        ["first_name", "surname"],
        [["first_name", "surname"]],
        ["sex"],
    )
    field_index = {
        (row["auxiliary_value"], row["column_name"]): row
        for row in subgroup_fields
    }
    assert field_index[("F", "first_name")]["usable_completeness_gap"] == (
        pytest.approx(0.25)
    )
    assert field_index[("M", "first_name")]["usable_completeness_gap"] == (
        pytest.approx(-0.25)
    )
    assert len(subgroup_combinations) == 2
    assert any(row["assessment_level"] == "individual PII" for row in equity)
    assert any(row["assessment_level"] == "combined PII" for row in equity)
    assert all(row["largest_adverse_gap"] <= 0 for row in equity)

    empty_group_sdf = test_helpers[dialect].db_api().register(
        {"field": [None, None, "A"], "group": ["empty", "empty", "observed"]}
    )
    empty_group_quality = subgroup_linkage_column_quality_data(
        empty_group_sdf, ["field"], ["group"]
    )
    empty_row = next(
        row for row in empty_group_quality if row["auxiliary_value"] == "empty"
    )
    assert empty_row["top_5pct_values_record_share"] == pytest.approx(0.0)

@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_dataset_linkage_overview_includes_id_and_duplicate_checks(
    dialect, test_helpers
):
    sdf = _fixture(test_helpers[dialect].db_api())
    overview = dataset_linkage_overview_data(
        sdf,
        ["first_name", "surname"],
        record_id_column="record_id",
        duplicate_columns=["first_name", "surname", "sex"],
    )[0]
    assert overview["total_rows"] == 8
    assert overview["complete_pii_share"] == pytest.approx(6 / 8)
    assert overview["usable_pii_share"] == pytest.approx(7 / 8)
    assert overview["exact_duplicate_records"] == 4
    assert overview["record_id_missing_rows"] == 0
    assert overview["record_id_duplicate_records"] == 0


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_cardinality_buckets_and_exact_duplicates(dialect, test_helpers):
    sdf = _fixture(test_helpers[dialect].db_api())
    distribution = combination_size_distribution_data(
        sdf, [["first_name", "surname"]]
    )
    duplicates = exact_duplicate_data(sdf, ["first_name", "surname", "sex"])

    buckets = {row["cardinality_bucket"]: row for row in distribution}
    assert buckets["1"]["signature_groups"] == 3
    assert buckets["1"]["records"] == 3
    assert buckets["2"]["signature_groups"] == 2
    assert buckets["2"]["records"] == 4
    duplicate = duplicates[0]
    assert duplicate["duplicate_groups"] == 2
    assert duplicate["records_in_duplicate_groups"] == 4
    assert duplicate["duplicate_record_share"] == pytest.approx(0.5)
    assert duplicate["excess_duplicate_rows"] == 2
    assert duplicate["largest_duplicate_group"] == 2


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_profile_reference_comparison_and_all_chart_specs(dialect, test_helpers):
    sdf = _fixture(test_helpers[dialect].db_api())
    profile = build_pre_linkage_profile(
        sdf,
        columns=["first_name", "surname"],
        column_combinations=[["first_name", "surname"], ["surname"]],
        auxiliary_columns=["sex"],
        duplicate_columns=["first_name", "surname", "sex"],
        record_id_column="record_id",
        maximum_curve_points=20,
    )
    assert profile["dataset_overview"]
    assert profile["subgroup_linkage_column_quality"]
    assert profile["subgroup_combination_ambiguity"]
    assert profile["subgroup_equity_gaps"]
    reference = {key: value for key, value in profile.items()}
    reference["column_quality"] = [dict(row) for row in profile["column_quality"]]
    reference["column_quality"][0]["usable_completeness"] = 1.0

    comparison = compare_pre_linkage_profiles(profile, reference)
    changed = next(
        row
        for row in comparison
        if row["comparison_section"] == "column_quality"
        and row["item"] == "first_name"
        and row["metric"] == "usable_completeness"
    )
    assert changed["difference"] == pytest.approx(-0.25)

    charts = [
        dataset_linkage_overview_chart(
            sdf,
            ["first_name", "surname"],
            record_id_column="record_id",
            duplicate_columns=["first_name", "surname", "sex"],
        ),
        value_concentration_chart(sdf, ["first_name", "surname"]),
        value_dominance_distribution_chart(sdf, ["first_name", "surname"]),
        missingness_by_column_chart(sdf, ["first_name", "surname"]),
        missingness_pattern_chart(sdf, ["first_name", "surname"]),
        subgroup_missingness_chart(sdf, ["first_name", "surname"], ["sex"]),
        subgroup_linkage_column_quality_chart(
            sdf, ["first_name", "surname"], ["sex"]
        ),
        subgroup_combination_ambiguity_chart(
            sdf, [["first_name", "surname"]], ["sex"]
        ),
        subgroup_equity_gap_chart(
            sdf, ["first_name"], [["first_name", "surname"]], ["sex"]
        ),
        subgroup_value_concentration_chart(sdf, ["first_name"], ["sex"]),
        subgroup_value_dominance_distribution_chart(
            sdf, ["first_name"], ["sex"]
        ),
        combination_size_distribution_chart(
            sdf, [["first_name", "surname"], ["surname"]]
        ),
        exact_duplicate_chart(sdf, ["first_name", "surname", "sex"]),
        pre_linkage_reference_comparison_chart(profile, reference),
        reference_value_concentration_chart(profile, reference),
        reference_value_dominance_distribution_chart(profile, reference),
    ]
    for chart in charts:
        assert chart.altair_chart.to_dict()["title"]["text"]
