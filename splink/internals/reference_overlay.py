from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from splink.internals.charts import ChartRecord, SplinkChart


class ReferenceValueConcentrationChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "reference_value_concentration.json"


class ReferenceValueDominanceDistributionChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "reference_value_dominance_distribution.json"


def _section_dataset_names(profile: Mapping[str, Any], section: str) -> list[str]:
    return list(
        dict.fromkeys(
            str(row["source_dataset"]) for row in profile.get(section, [])
        )
    )


def reference_profile_overlay_data(
    current_profile: Mapping[str, Any],
    reference_profile: Mapping[str, Any],
    *,
    section: str,
    current_dataset: str | None = None,
    reference_dataset: str | None = None,
    current_label: str = "Current",
    reference_label: str = "Reference",
) -> list[dict[str, Any]]:
    """Combine comparable aggregate profile sections for direct chart overlay."""
    supported = {"value_concentration", "value_dominance_distribution"}
    if section not in supported:
        raise ValueError(f"section must be one of: {', '.join(sorted(supported))}")
    current_names = _section_dataset_names(current_profile, section)
    reference_names = _section_dataset_names(reference_profile, section)
    if not current_names or not reference_names:
        raise ValueError(f"Both profiles must contain {section!r} records.")
    current_dataset = current_dataset or current_names[0]
    reference_dataset = reference_dataset or reference_names[0]
    if current_dataset not in current_names:
        raise ValueError(f"Unknown current_dataset: {current_dataset}")
    if reference_dataset not in reference_names:
        raise ValueError(f"Unknown reference_dataset: {reference_dataset}")

    output: list[dict[str, Any]] = []
    for profile, dataset, label, role in (
        (current_profile, current_dataset, current_label, "current"),
        (reference_profile, reference_dataset, reference_label, "reference"),
    ):
        for original in profile[section]:
            if str(original["source_dataset"]) != dataset:
                continue
            row = dict(original)
            row["profile_label"] = label
            row["profile_role"] = role
            output.append(row)
    return output


def reference_value_concentration_chart(
    current_profile: Mapping[str, Any],
    reference_profile: Mapping[str, Any],
    **kwargs,
) -> ReferenceValueConcentrationChart:
    """Overlay current and reference cumulative value-concentration curves."""
    return ReferenceValueConcentrationChart(
        reference_profile_overlay_data(
            current_profile,
            reference_profile,
            section="value_concentration",
            **kwargs,
        )
    )


def reference_value_dominance_distribution_chart(
    current_profile: Mapping[str, Any],
    reference_profile: Mapping[str, Any],
    **kwargs,
) -> ReferenceValueDominanceDistributionChart:
    """Overlay current and reference full value-frequency distributions."""
    return ReferenceValueDominanceDistributionChart(
        reference_profile_overlay_data(
            current_profile,
            reference_profile,
            section="value_dominance_distribution",
            **kwargs,
        )
    )
