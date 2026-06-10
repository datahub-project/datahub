"""
Grasshopper-based DataHub search load tests — pytest entry point.

All test configuration (users, runtime, thresholds) is defined in
scenarios/search_scenarios.yaml.  The complete_configuration fixture
is provided automatically by the locust-grasshopper pytest plugin.

Run modes
---------
Smoke (5 users, 60 s, p90 gates enabled):
  cd e2e-test/perf
  pytest implementation_2/tests/test_search_load.py \\
      --scenario_file=implementation_2/scenarios/search_scenarios.yaml \\
      --scenario_name=search_smoke \\
      -H http://localhost:8080

Ramp (200 users, 10 min):
  pytest implementation_2/tests/test_search_load.py \\
      --scenario_file=implementation_2/scenarios/search_scenarios.yaml \\
      --scenario_name=search_ramp \\
      -H http://localhost:8080

Stress (500 users, 20 min):
  pytest implementation_2/tests/test_search_load.py \\
      --scenario_file=implementation_2/scenarios/search_scenarios.yaml \\
      --scenario_name=search_stress \\
      -H http://localhost:8080

All smoke scenarios (tag filter):
  pytest implementation_2/tests/test_search_load.py \\
      --scenario_file=implementation_2/scenarios/search_scenarios.yaml \\
      --tags=smoke \\
      -H http://localhost:8080

With InfluxDB + Grafana metrics export:
  pytest implementation_2/tests/test_search_load.py \\
      --scenario_file=implementation_2/scenarios/search_scenarios.yaml \\
      --scenario_name=search_ramp \\
      -H http://localhost:8080 \\
      --influx_host=localhost

Auth:
  export DATAHUB_GMS_TOKEN=<your-PAT>
  (omit for local no-auth dev instances — actor-spoof header is used automatically)
"""

from grasshopper.lib.grasshopper import Grasshopper

from journeys.search_journey import GlobalSearchJourney


def test_search_load(complete_configuration: dict) -> None:
    GlobalSearchJourney.update_incoming_scenario_args(complete_configuration)
    Grasshopper.launch_test(GlobalSearchJourney, **complete_configuration)
