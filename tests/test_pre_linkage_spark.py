import pytest

from splink.exploratory import (
    combination_size_distribution_chart,
    combination_size_distribution_data,
    dataset_linkage_overview_data,
    missingness_pattern_data,
    subgroup_combination_ambiguity_data,
    subgroup_linkage_column_quality_data,
    value_concentration_data,
)


pytestmark = [pytest.mark.spark, pytest.mark.spark_only]


def test_pre_linkage_assessment_runs_on_spark(spark_api):
    """Exercise the main SQL shapes through Splink's real Spark backend."""
    sdf = spark_api.register(
        {
            "record_id": list(range(1, 9)),
            "first_name": ["A", "A", "B", None, "", "C", "C", "D"],
            "surname": ["X", "X", "Y", "Y", None, "Z", "Z", "W"],
            "sex": ["F", "F", "M", "M", "M", "F", "F", "M"],
        }
    )

    overview = dataset_linkage_overview_data(
        sdf,
        ["first_name", "surname"],
        record_id_column="record_id",
        duplicate_columns=["first_name", "surname", "sex"],
    )[0]
    assert overview["complete_pii_share"] == pytest.approx(6 / 8)
    assert overview["record_id_duplicate_records"] == 0

    cardinality = combination_size_distribution_data(
        sdf,
        [["first_name", "surname"], ["surname"]],
    )
    assert {row["cardinality_bucket"] for row in cardinality} == {"1", "2"}
    assert {row["combination_order"] for row in cardinality} == {0, 1}
    assert combination_size_distribution_chart(
        sdf, [["first_name", "surname"], ["surname"]]
    ).altair_chart.to_dict()["title"]

    patterns = missingness_pattern_data(sdf, ["first_name", "surname"])
    assert sum(int(row["records"]) for row in patterns) == 8

    concentration = value_concentration_data(sdf, ["first_name"])
    assert concentration[-1]["cumulative_record_share"] == pytest.approx(1.0)

    subgroup_fields = subgroup_linkage_column_quality_data(
        sdf, ["first_name", "surname"], ["sex"]
    )
    assert {row["auxiliary_value"] for row in subgroup_fields} == {"F", "M"}

    subgroup_combinations = subgroup_combination_ambiguity_data(
        sdf, [["first_name", "surname"]], ["sex"]
    )
    assert len(subgroup_combinations) == 2
