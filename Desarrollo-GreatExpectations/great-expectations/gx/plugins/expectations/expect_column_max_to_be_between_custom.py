from datetime import datetime, timedelta
from typing import Dict, Optional

from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)


# Esta clase define una Métrica para tu Expectativa
class ColumnOver18(ColumnAggregateMetricProvider):
    metric_name = "column.over_18"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _condition(cls, column, **kwargs):
        """Verifica si la fecha de nacimiento es mayor a 18 años."""
        eighteen_years_ago = datetime.today() - timedelta(days=18 * 365)
        return column.apply(lambda x: x <= eighteen_years_ago)


# Esta clase define la Expectativa
class ExpectColumnMinEighteenYearsOld(ColumnAggregateExpectation):
    examples = [
        {
            "data": {
                "fecha_nacimiento": ["2000-05-15", "2010-08-20", "1995-12-10"]
            },
            "tests": [
                {
                    "title": "Test de edad mínima",
                    "include_in_gallery": True,
                    "input": {"column": "fecha_nacimiento", "mostly": 0.9},
                    "output": {"success": True},
                }
            ],
        }
    ]

    # Las dependencias de la métrica para esta expectativa
    metric_dependencies = "column.over_18"

    # Estos son los parámetros que la Expectativa puede recibir como parte de la configuración.
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    def validate_configuration(
            self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)

        # Si la configuración no se pasa directamente, tomamos la configuración predeterminada
        configuration = configuration or self.configuration

        # Verifica si los campos que se esperan están presentes en la configuración
        if "min_value" not in configuration.kwargs:
            raise ValueError("min_value is required for this Expectation")
        if "max_value" not in configuration.kwargs:
            raise ValueError("max_value is required for this Expectation")
        if "strict_min" not in configuration.kwargs:
            raise ValueError("strict_min is required for this Expectation")
        if "strict_max" not in configuration.kwargs:
            raise ValueError("strict_max is required for this Expectation")

    def _validate(
            self,
            metrics: Dict,
            runtime_configuration: Optional[dict] = None,
            execution_engine: ExecutionEngine = None,
    ):
        """Valida que todas las fechas de nacimiento sean mayores de 18 años."""
        over_18 = metrics["column.over_18"]

        success_kwargs = self._get_success_kwargs()
        min_value = success_kwargs.get("min_value", 18)
        max_value = success_kwargs.get("max_value", 150)
        strict_min = success_kwargs.get("strict_min", True)
        strict_max = success_kwargs.get("strict_max", False)

        # Verificar que todas las fechas estén por encima de 18 años
        success = over_18.all()  # Verifica que todos los valores sean mayores de 18

        return {"success": success, "result": {"observed_value": over_18.sum()}}

    library_metadata = {
        "tags": [],
        "contributors": [
            "@your_name_here",
        ],
    }


if __name__ == "__main__":
    # Configuración de la Expectativa
    expectation_config = ExpectationConfiguration(
        type="expect_column_min_eighteen_years_old",  # Añadido el tipo de expectativa aquí
        kwargs={
            "column": "fecha_nacimiento",
            "min_value": 18,
            "max_value": 150,
            "strict_min": True,
            "strict_max": False,
        },
    )

    # Valida la configuración
    ExpectColumnMinEighteenYearsOld.validate_configuration(expectation_config)

    # Crea la expectativa con la configuración adecuada
    expectation = ExpectColumnMinEighteenYearsOld(configuration=expectation_config)
    expectation.print_diagnostic_checklist()
