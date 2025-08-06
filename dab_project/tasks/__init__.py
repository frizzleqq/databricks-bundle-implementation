from .base_task import Task

# Import all task implementations to register them with the base class
from .bronze_accuweather import BronzeAccuweatherTask
from .bronze_nyctaxi import BronzeTaxiTask
from .silver_nyctaxi_aggregated import SilverTaxiAggTask
