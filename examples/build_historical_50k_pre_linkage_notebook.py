from pathlib import Path
from textwrap import dedent

import json


def markdown(text: str):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": dedent(text).strip().splitlines(keepends=True),
    }


def code(text: str):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": dedent(text).strip().splitlines(keepends=True),
    }


cells = [
    markdown(
        """
        # Historical 50k pre-linkage assessment

        This notebook is an interactive, Splink-native version of the first part
        of the pre-linkage report. It examines whether identifiers are present and
        distinguishing **before** fitting a linkage model.

        The example uses one complete dataset because the task is deduplication.
        Splink calls it the historical 50k dataset, although it contains 50,578
        rows.
        """
    ),
    markdown(
        """
        ## 1. Choose what to assess

        Edit these lists to test different identifiers and progressively relaxed
        exact combinations. `BLANK_AS_NULL=True` treats empty and whitespace-only
        strings as unavailable.
        """
    ),
    code(
        """
        from pathlib import Path
        import importlib
        import json
        import sys

        import pandas as pd

        working_directory = Path.cwd().resolve()
        candidates = [
            working_directory,
            working_directory / "splink_prelinkage",
            *working_directory.parents,
        ]
        project_root = next(
            (
                path
                for path in candidates
                if (path / "splink").is_dir() and (path / "pyproject.toml").is_file()
            ),
            None,
        )
        if project_root is None:
            raise RuntimeError(
                "Could not locate the splink_prelinkage repository. Start Jupyter "
                "from that repository or its parent directory."
            )
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

        # Reload the local checkout when this notebook is rerun after source edits.
        importlib.invalidate_caches()
        for module_name in list(sys.modules):
            if module_name == "splink" or module_name.startswith("splink."):
                del sys.modules[module_name]

        from splink import DuckDBAPI, splink_datasets
        from splink.exploratory import (
            build_pre_linkage_profile,
            combination_ambiguity_chart,
            combination_ambiguity_data,
            combination_size_distribution_chart,
            dataset_linkage_overview_chart,
            dataset_linkage_overview_data,
            exact_duplicate_chart,
            exact_duplicate_data,
            linkage_column_quality_chart,
            linkage_column_quality_data,
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
            subgroup_missingness_summary_data,
            subgroup_value_concentration_chart,
            subgroup_value_dominance_distribution_chart,
            value_concentration_chart,
            value_dominance_distribution_chart,
        )

        LINKAGE_COLUMNS = [
            "first_name",
            "surname",
            "dob",
            "postcode_fake",
            "birth_place",
            "occupation",
        ]
        COLUMN_COMBINATIONS = [
            ["first_name", "surname", "dob", "postcode_fake"],
            ["surname", "dob", "postcode_fake"],
            ["first_name", "dob", "postcode_fake"],
            ["dob", "postcode_fake"],
            ["surname", "dob"],
            ["postcode_fake"],
        ]
        SUBGROUP_COMBINATIONS = [
            ["first_name", "surname", "dob", "postcode_fake"],
            ["dob", "postcode_fake"],
        ]
        DISTRIBUTION_COLUMNS = ["first_name", "surname", "postcode_fake"]
        AUXILIARY_COLUMNS = ["gender"]
        DUPLICATE_COLUMNS = [
            "first_name",
            "surname",
            "dob",
            "postcode_fake",
            "birth_place",
            "occupation",
            "gender",
        ]
        RECORD_ID_COLUMN = "unique_id"
        # Set this to a JSON profile created by a previous approved assessment.
        REFERENCE_PROFILE_PATH = None
        BLANK_AS_NULL = True
        """
    ),
    markdown(
        """
        ## 2. Load the data locally

        The identifiers stay in DuckDB. The assessment outputs contain column
        names and aggregate counts, but not observed names, dates, postcodes or
        identifier combinations.
        """
    ),
    code(
        """
        db_api = DuckDBAPI()
        historical_50k = splink_datasets.historical_50k
        records = db_api.register(historical_50k)

        pd.DataFrame(
            {
                "measure": ["Rows", "Columns", "Assessment purpose"],
                "value": [
                    f"{historical_50k.num_rows:,}",
                    str(len(historical_50k.column_names)),
                    "Pre-linkage assessment for deduplication",
                ],
            }
        )
        """
    ),
    markdown(
        """
        ## 3. Declared schema and roles

        This declaration tells the reader how fields are used. It does not alter
        the source data. `cluster` is a known duplicate-group label retained for
        later evaluation; it is not used in this assessment.
        """
    ),
    code(
        """
        roles = {
            "unique_id": ("record identifier", "Identifies a submitted row."),
            "cluster": ("evaluation label", "Known duplicate group; not used here."),
            "first_name": ("linkage column", "Candidate personal identifier."),
            "surname": ("linkage column", "Candidate personal identifier."),
            "dob": ("linkage column", "Candidate date-of-birth identifier."),
            "postcode_fake": ("linkage column", "Synthetic postcode-like identifier."),
            "birth_place": ("linkage column", "Candidate place identifier."),
            "occupation": ("linkage column", "Candidate contextual identifier."),
            "gender": ("auxiliary variable", "Available for subgroup assessment."),
            "full_name": ("derived identifier", "Combined name representation."),
            "first_and_surname": ("derived identifier", "Combined name representation."),
        }
        schema_rows = []
        for field, data_type in zip(historical_50k.column_names, historical_50k.schema.types):
            role, meaning = roles.get(field, ("other", "Not declared for this example."))
            schema_rows.append(
                {
                    "field": field,
                    "data_type": str(data_type),
                    "declared_role": role,
                    "meaning": meaning,
                }
            )
        pd.DataFrame(schema_rows)
        """
    ),
    markdown(
        """
        ## 4. Dataset-level linkage overview

        Start with the whole-dataset envelope before examining individual PII.
        **All PII complete** describes rows available to strict exact rules.
        **At least one PII usable** shows whether any declared identifier remains.
        **Rows with a unique full signature** describes rows distinguished without
        ties by the complete declared combination. **Maximum distinct 1:1
        assignments** allows one assignment per usable signature and is therefore
        a structural ceiling, not an expected match rate.

        Record-ID checks concern the submitted row identifier. Exact duplicate
        checks use `DUPLICATE_COLUMNS` and exclude the row ID and truth label.
        """
    ),
    code(
        """
        dataset_overview_records = dataset_linkage_overview_data(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            record_id_column=RECORD_ID_COLUMN,
            duplicate_columns=DUPLICATE_COLUMNS,
            blank_as_null=BLANK_AS_NULL,
        )
        dataset_overview = pd.DataFrame(dataset_overview_records)
        dataset_overview.style.format(
            {
                "complete_pii_share": "{:.1%}",
                "usable_pii_share": "{:.1%}",
                "unique_full_signature_share": "{:.1%}",
                "ambiguous_full_signature_share": "{:.1%}",
                "maximum_one_to_one_assignment_share": "{:.1%}",
                "exact_duplicate_record_share": "{:.1%}",
            }
        )
        """
    ),
    code(
        """
        dataset_linkage_overview_chart(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            record_id_column=RECORD_ID_COLUMN,
            duplicate_columns=DUPLICATE_COLUMNS,
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    markdown(
        """
        ## 4. Quality of individual linkage columns

        - **Usable completeness:** how often a field contains something usable.
        - **Distinct-value rate:** distinct values divided by observed rows.
        - **Singleton-record share:** rows whose value occurs once.
        - **Repeated-record share:** rows whose value is shared with other rows.
        - **Largest-value share:** concentration in the most common value.

        No single measure determines linkability. A field can be complete but
        weakly distinguishing, or distinctive but frequently missing.
        """
    ),
    code(
        """
        column_quality_records = linkage_column_quality_data(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        column_quality = pd.DataFrame(column_quality_records)
        quality_view = column_quality[
            [
                "column_name",
                "total_rows",
                "observed_rows",
                "usable_completeness",
                "distinct_values",
                "distinct_value_rate",
                "singleton_record_share",
                "repeated_record_share",
                "largest_value_share",
                "top_5pct_values_record_share",
            ]
        ].copy()
        quality_view.style.format(
            {
                "total_rows": "{:,.0f}",
                "observed_rows": "{:,.0f}",
                "usable_completeness": "{:.1%}",
                "distinct_values": "{:,.0f}",
                "distinct_value_rate": "{:.1%}",
                "singleton_record_share": "{:.1%}",
                "repeated_record_share": "{:.1%}",
                "largest_value_share": "{:.1%}",
                "top_5pct_values_record_share": "{:.1%}",
            }
        )
        """
    ),
    code(
        """
        linkage_column_quality_chart(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        lowest_completeness = column_quality.loc[column_quality["usable_completeness"].idxmin()]
        highest_repetition = column_quality.loc[column_quality["repeated_record_share"].idxmax()]
        highest_concentration = column_quality.loc[column_quality["largest_value_share"].idxmax()]

        pd.DataFrame(
            [
                {
                    "question": "Which assessed field is least complete?",
                    "finding": lowest_completeness["column_name"],
                    "evidence": f'{lowest_completeness["usable_completeness"]:.1%} usable',
                    "linkage implication": "Missing values can prevent true pairs agreeing on this field.",
                },
                {
                    "question": "Which field places most rows in repeated values?",
                    "finding": highest_repetition["column_name"],
                    "evidence": f'{highest_repetition["repeated_record_share"]:.1%} repeated',
                    "linkage implication": "Other identifiers are needed to distinguish shared values.",
                },
                {
                    "question": "Which field has the largest single-value concentration?",
                    "finding": highest_concentration["column_name"],
                    "evidence": f'{highest_concentration["largest_value_share"]:.1%} in its largest group',
                    "linkage implication": "A common value can generate many candidates if used alone.",
                },
            ]
        )
        """
    ),
    markdown(
        """
        ## 5. Combination ambiguity and relaxed exact keys

        An exact signature is the selected field combination on a row. It is
        **unique** if it occurs once and **ambiguous** if it occurs on several
        rows. Removing fields shows what happens when an exact key is relaxed.

        This is a within-dataset diagnostic, not a prediction of linkage accuracy.
        """
    ),
    code(
        """
        ambiguity_records = combination_ambiguity_data(
            records,
            column_combinations=COLUMN_COMBINATIONS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        ambiguity = pd.DataFrame(ambiguity_records)
        ambiguity_view = ambiguity[
            [
                "column_combination",
                "complete_record_share",
                "unique_record_share",
                "ambiguous_record_share",
                "unusable_record_share",
                "largest_signature_group",
                "maximum_one_to_one_assignment_share",
            ]
        ].copy()
        ambiguity_view.style.format(
            {
                "complete_record_share": "{:.1%}",
                "unique_record_share": "{:.1%}",
                "ambiguous_record_share": "{:.1%}",
                "unusable_record_share": "{:.1%}",
                "largest_signature_group": "{:,.0f}",
                "maximum_one_to_one_assignment_share": "{:.1%}",
            }
        )
        """
    ),
    code(
        """
        combination_ambiguity_chart(
            records,
            column_combinations=COLUMN_COMBINATIONS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    markdown(
        """
        ## 6. Cumulative uniqueness and value concentration

        The cumulative curve ranks observed values from most to least frequent.
        It shows how much of the dataset is occupied by a given share of distinct
        values. The full dominance distribution then shows how many values and
        records occur at each frequency. Observed identifier values are not
        returned.
        """
    ),
    code(
        """
        value_concentration_chart(
            records,
            columns=DISTRIBUTION_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        value_dominance_distribution_chart(
            records,
            columns=DISTRIBUTION_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    markdown(
        """
        ## 7. Missingness by PII and co-missingness pattern

        Per-column missingness identifies which fields are unavailable.
        Co-missingness identifies fields that are absent together. The complete
        pattern table is ordered with the most common pattern first; the chart
        displays the leading patterns for readability.
        """
    ),
    code(
        """
        missingness_by_column_records = missingness_by_column_data(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        pd.DataFrame(missingness_by_column_records).style.format(
            {"missing_record_share": "{:.1%}"}
        )
        """
    ),
    code(
        """
        missingness_by_column_chart(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        missingness_patterns = missingness_pattern_data(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        pd.DataFrame(missingness_patterns).head(15).style.format(
            {"record_share": "{:.1%}"}
        )
        """
    ),
    code(
        """
        missingness_pattern_chart(
            records,
            columns=LINKAGE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
            top_n=12,
        )
        """
    ),
    code(
        """
        pd.DataFrame(
            missingness_by_count_data(
                records,
                columns=LINKAGE_COLUMNS,
                table_names_for_chart=["Historical 50k"],
                blank_as_null=BLANK_AS_NULL,
            )
        ).style.format({"record_share": "{:.1%}"})
        """
    ),
    markdown(
        """
        ## 8. Full subgroup equity assessment

        `AUXILIARY_COLUMNS` controls the stratification. The example uses gender;
        add or replace columns when another approved grouping variable is more
        relevant. This compares completeness patterns, not linkage outcomes.

        Gaps are subgroup minus dataset average. The equity screen uses descriptive
        triage thresholds of 2, 5 and 10 percentage points; these are not
        statistical tests or evidence of realised linkage bias.
        """
    ),
    code(
        """
        subgroup_missingness = subgroup_missingness_summary_data(
            records,
            columns=LINKAGE_COLUMNS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        pd.DataFrame(subgroup_missingness).style.format(
            {
                "any_missing_share": "{:.1%}",
                "most_common_incomplete_pattern_share": "{:.1%}",
            }
        )
        """
    ),
    code(
        """
        subgroup_missingness_chart(
            records,
            columns=LINKAGE_COLUMNS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        subgroup_fields = subgroup_linkage_column_quality_data(
            records,
            columns=LINKAGE_COLUMNS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        subgroup_field_view = pd.DataFrame(subgroup_fields)[
            [
                "auxiliary_column",
                "auxiliary_value",
                "column_name",
                "group_records",
                "usable_completeness",
                "usable_completeness_gap",
                "repeated_record_share",
                "repeated_record_share_gap",
                "top_5pct_values_record_share",
                "top_5pct_values_record_share_gap",
            ]
        ]
        subgroup_field_view.style.format(
            {
                column: "{:+.1%}" if column.endswith("_gap") else "{:.1%}"
                for column in subgroup_field_view
                if "share" in column or "completeness" in column
            }
        )
        """
    ),
    code(
        """
        subgroup_linkage_column_quality_chart(
            records,
            columns=LINKAGE_COLUMNS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        subgroup_combinations = subgroup_combination_ambiguity_data(
            records,
            column_combinations=SUBGROUP_COMBINATIONS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        pd.DataFrame(subgroup_combinations)[
            [
                "auxiliary_value",
                "column_combination",
                "group_records",
                "complete_record_share",
                "unique_record_share",
                "ambiguous_record_share",
                "maximum_one_to_one_assignment_share",
                "maximum_one_to_one_assignment_share_gap",
            ]
        ].style.format(
            {
                column: "{:+.1%}" if column.endswith("_gap") else "{:.1%}"
                for column in pd.DataFrame(subgroup_combinations)
                if "share" in column
            }
        )
        """
    ),
    code(
        """
        subgroup_combination_ambiguity_chart(
            records,
            column_combinations=SUBGROUP_COMBINATIONS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        subgroup_equity = subgroup_equity_gap_data(
            records,
            columns=LINKAGE_COLUMNS,
            column_combinations=SUBGROUP_COMBINATIONS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        pd.DataFrame(subgroup_equity).head(30).style.format(
            {
                "group_record_share": "{:.1%}",
                "largest_adverse_gap": "{:+.1%}",
                "adverse_gap_magnitude": "{:.1%}",
            }
        )
        """
    ),
    code(
        """
        subgroup_equity_gap_chart(
            records,
            columns=LINKAGE_COLUMNS,
            column_combinations=SUBGROUP_COMBINATIONS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        subgroup_value_concentration_chart(
            records,
            columns=DISTRIBUTION_COLUMNS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        subgroup_value_dominance_distribution_chart(
            records,
            columns=DISTRIBUTION_COLUMNS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    markdown(
        """
        ## 9. Combination cardinality and exact duplicate rows

        Cardinality buckets show the number of rows sharing each exact linkage
        signature. The duplicate assessment uses the declared submitted columns
        exactly as stored and excludes the row identifier and evaluation label.
        """
    ),
    code(
        """
        combination_size_distribution_chart(
            records,
            column_combinations=COLUMN_COMBINATIONS,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        """
    ),
    code(
        """
        duplicate_records = exact_duplicate_data(
            records,
            columns=DUPLICATE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
        )
        pd.DataFrame(duplicate_records).style.format(
            {"duplicate_record_share": "{:.1%}"}
        )
        """
    ),
    code(
        """
        exact_duplicate_chart(
            records,
            columns=DUPLICATE_COLUMNS,
            table_names_for_chart=["Historical 50k"],
        )
        """
    ),
    markdown(
        """
        ## 10. Reusable profile and optional reference comparison

        The profile combines all aggregate sections in a serialisable dictionary.
        A reference must be another profile produced with compatible column and
        missingness declarations, such as a previous spine extract. Differences
        are current minus reference and should not be interpreted as data-quality
        change without considering population and coverage changes.
        """
    ),
    code(
        """
        profile = build_pre_linkage_profile(
            records,
            columns=LINKAGE_COLUMNS,
            column_combinations=COLUMN_COMBINATIONS,
            auxiliary_columns=AUXILIARY_COLUMNS,
            duplicate_columns=DUPLICATE_COLUMNS,
            record_id_column=RECORD_ID_COLUMN,
            table_names_for_chart=["Historical 50k"],
            blank_as_null=BLANK_AS_NULL,
        )
        list(profile)
        """
    ),
    code(
        """
        if REFERENCE_PROFILE_PATH:
            with Path(REFERENCE_PROFILE_PATH).open(encoding="utf-8") as handle:
                reference_profile = json.load(handle)
            display(
                pre_linkage_reference_comparison_chart(
                    profile,
                    reference_profile,
                    current_label="Current submission",
                    reference_label="Reference",
                )
            )
            display(
                reference_value_concentration_chart(
                    profile,
                    reference_profile,
                    current_label="Current submission",
                    reference_label="Reference",
                )
            )
            display(
                reference_value_dominance_distribution_chart(
                    profile,
                    reference_profile,
                    current_label="Current submission",
                    reference_label="Reference",
                )
            )
        else:
            print("Set REFERENCE_PROFILE_PATH to compare with an aggregate reference.")
        """
    ),
    markdown(
        """
        ## 11. What this means for the next linkage stage

        1. Avoid relying on a field that is frequently missing.
        2. Combine common fields with more distinguishing identifiers.
        3. Test relaxed combinations with Splink's blocking analysis before scoring.
        4. Assess subgroup differences where appropriate auxiliary variables exist.
        5. Evaluate the fitted linkage model separately using appropriate evidence.

        These are structural opportunities and constraints. They are not pass/fail
        rules and do not establish precision or recall.
        """
    ),
    markdown(
        """
        ## 12. Optional export

        Set `EXPORT_OUTPUTS=True` below to save the reusable aggregate profile,
        selected CSV files and standalone Splink charts. The existing `prelinkage_assessment` project
        still contains the fuller composite HTML report. It has not been deleted
        or replaced by this notebook; only the generic core is being redeveloped
        inside Splink at this stage.
        """
    ),
    code(
        """
        EXPORT_OUTPUTS = False
        OUTPUT_DIR = project_root / "example_outputs" / "historical_50k_pre_linkage"

        if EXPORT_OUTPUTS:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            dataset_overview.to_csv(
                OUTPUT_DIR / "dataset_linkage_overview.csv", index=False
            )
            column_quality.to_csv(OUTPUT_DIR / "linkage_column_quality.csv", index=False)
            ambiguity.to_csv(OUTPUT_DIR / "combination_ambiguity.csv", index=False)
            pd.DataFrame(missingness_by_column_records).to_csv(
                OUTPUT_DIR / "missingness_by_column.csv", index=False
            )
            pd.DataFrame(missingness_patterns).to_csv(
                OUTPUT_DIR / "missingness_patterns.csv", index=False
            )
            pd.DataFrame(subgroup_missingness).to_csv(
                OUTPUT_DIR / "subgroup_missingness.csv", index=False
            )
            pd.DataFrame(duplicate_records).to_csv(OUTPUT_DIR / "exact_duplicates.csv", index=False)
            with (OUTPUT_DIR / "pre_linkage_metrics.json").open("w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "dataset": "Splink historical 50k",
                        "records": historical_50k.num_rows,
                        "linkage_column_quality": column_quality_records,
                        "combination_ambiguity": ambiguity_records,
                    },
                    handle,
                    indent=2,
                )
            with (OUTPUT_DIR / "pre_linkage_profile.json").open("w", encoding="utf-8") as handle:
                json.dump(profile, handle, indent=2)
            linkage_column_quality_chart(
                records,
                columns=LINKAGE_COLUMNS,
                table_names_for_chart=["Historical 50k"],
            ).save_offline_chart(
                OUTPUT_DIR / "linkage_column_quality.html",
                overwrite=True,
                print_msg=False,
            )
            combination_ambiguity_chart(
                records,
                column_combinations=COLUMN_COMBINATIONS,
                table_names_for_chart=["Historical 50k"],
            ).save_offline_chart(
                OUTPUT_DIR / "combination_ambiguity.html",
                overwrite=True,
                print_msg=False,
            )
            print(f"Outputs written to {OUTPUT_DIR}")
        else:
            print("Nothing exported. Set EXPORT_OUTPUTS=True to save outputs.")
        """
    ),
]

SECTION_ORDER = [
    "Choose what to assess",
    "Load the data locally",
    "Declared schema and roles",
    "Dataset-level linkage overview",
    "Quality of individual linkage columns",
    "Missingness by PII and co-missingness pattern",
    "Cumulative uniqueness and value concentration",
    "Combination ambiguity and relaxed exact keys",
    "Combination cardinality and exact duplicate rows",
    "Full subgroup equity assessment",
    "Reusable profile and optional reference comparison",
    "What this means for the next linkage stage",
    "Optional export",
]

def section_title(cell):
    if cell["cell_type"] != "markdown":
        return None
    for line in cell["source"]:
        stripped = line.strip()
        if stripped.startswith("## ") and ". " in stripped:
            return stripped.split(". ", 1)[1]
    return None


preamble = []
sections = {}
current_section = None
for cell in cells:
    title = section_title(cell)
    if title is not None:
        current_section = title
        sections[current_section] = []
    if current_section is None:
        preamble.append(cell)
    else:
        sections[current_section].append(cell)

missing_sections = [title for title in SECTION_ORDER if title not in sections]
if missing_sections:
    raise RuntimeError(f"Notebook sections missing from generator: {missing_sections}")
cells = preamble + [
    cell for title in SECTION_ORDER for cell in sections[title]
]
for section_number, title in enumerate(SECTION_ORDER, start=1):
    heading = sections[title][0]
    for line_number, line in enumerate(heading["source"]):
        if line.strip().startswith("## "):
            suffix = "\n" if line.endswith("\n") else ""
            heading["source"][line_number] = f"## {section_number}. {title}{suffix}"
            break

for index, cell in enumerate(cells):
    cell["id"] = f"pre-linkage-{index:03d}"

notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3 (ipykernel)",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": "3"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

output_path = Path(__file__).with_name("historical_50k_pre_linkage.ipynb")
output_path.write_text(json.dumps(notebook, indent=1), encoding="utf-8")
print(output_path)
