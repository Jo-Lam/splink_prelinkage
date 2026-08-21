from splink.internals.charts import ChartRecord, SplinkChart


class LinkageColumnQualityChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "linkage_column_quality.json"


class CombinationAmbiguityChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "combination_ambiguity.json"
