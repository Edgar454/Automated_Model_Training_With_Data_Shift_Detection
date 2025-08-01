from typing import Optional

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)

class RainZeroWhenPrecipHoursZero(ColumnPairMapMetricProvider):
    """
    Custom metric to check that if precipitation_hours == 0, then rain_sum == 0
    """
    condition_metric_name = "column_pair_values.rain_zero_when_precip_zero"

    condition_domain_keys = ("column_A", "column_B")
    condition_value_keys = ()

    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        # column_A = precipitation_hours, column_B = rain_sum
        return ~((column_A == 0) & (column_B != 0))

class ExpectRainToBeZeroWhenPrecipitationHoursIsZero(ColumnPairMapExpectation):
    """
    Expect that if precipitation_hours == 0, then rain_sum == 0
    """

    map_metric = "column_pair_values.rain_zero_when_precip_zero"
    success_keys = ()
    default_kwarg_values = {}

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        assert configuration is not None
        return True

if __name__ == "__main__":
    ExpectRainToBeZeroWhenPrecipitationHoursIsZero().print_diagnostic_checklist()