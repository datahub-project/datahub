# Observability Tests

This directory contains integration tests for the datahub-integrations-service observability system.

## Overview

The observability system provides comprehensive metrics and instrumentation for:

- **Bot Operations**: Slack/Teams command execution, API calls, OAuth flows
- **GenAI Operations**: LLM cost tracking, token usage, provider metrics
- **Action Processing**: Event processing, asset tracking, pipeline stages
- **System Health**: HTTP requests, subprocess metrics, service health

## Test Structure

### Integration Tests (this directory)

**`test_bot_metrics.py`** - Bot observability metrics

- Tests for `BotPlatform`, `BotCommand`, and `OAuthFlow` enums
- Validates bot metric recording functions:
  - `record_datahub_query()` - DataHub API queries from bots
  - `record_slack_api_call()` - Slack API interactions
  - `record_teams_api_call()` - Teams API interactions
  - `record_oauth_flow()` - OAuth authentication flows
- Tests `datahub_query_tracker` context manager for automatic timing

**`test_phase3_metrics.py`** - Event and asset processing metrics

- Tests action execution metrics recording
- Validates event processing statistics:
  - `record_asset_processed()` - Assets processed during bootstrap/live stages
  - `record_asset_impacted()` - Assets modified by actions
  - `record_action_executed()` - Action execution counts
  - `record_event_processed()` - Event processing success/failure
  - `record_event_lag()` - Time between event creation and processing
- Integration tests for `ActionStageReport` and `EventProcessingStats`

### Unit Tests (`tests/unit/observability/`)

**Core Configuration & Initialization:**

- `test_config.py` - ObservabilityConfig settings and environment variables
- `test_otel_config.py` - OpenTelemetry initialization and configuration
- `test_graceful_degradation.py` - Fallback behavior when metrics unavailable

**Cost Tracking & GenAI:**

- `test_cost_improvements.py` - LLM pricing, Bedrock premiums, custom pricing
- `test_cost_utils.py` - Provider/model detection and normalization helpers

**Metrics Infrastructure:**

- `test_metrics_registry.py` - Metric definitions, bucket profiles, validation
- `test_instrumentation.py` - Decorators (@otel_instrument, @otel_duration, etc.)
- `test_subprocess_metrics_bridge.py` - Subprocess metrics aggregation

## Running Tests

### Run All Observability Tests

```bash
cd datahub-integrations-service
source venv/bin/activate

# Run all observability tests (unit + integration)
pytest tests/unit/observability/ tests/observability/ -v

# Run just integration tests (this directory)
pytest tests/observability/ -v

# Run specific test file
pytest tests/observability/test_bot_metrics.py -v
```

### Run with Coverage

```bash
pytest tests/unit/observability/ tests/observability/ --cov=src/datahub_integrations/observability --cov-report=html
```

## Smoke Tests

**IMPORTANT**: Comprehensive end-to-end smoke tests for the integrations service are available in the main DataHub repository:

```bash
cd /path/to/datahub/smoke-test
pytest tests/integrations/ -v
```

These smoke tests validate:

- Full service deployment and configuration
- Real bot interactions (Slack/Teams)
- GenAI cost tracking with actual LLM calls
- Metrics endpoint Prometheus format
- Dashboard accessibility and functionality

See the main DataHub repository's `smoke-test/` directory for setup instructions and test scenarios.

## Test Requirements

### Python Dependencies

All test dependencies are included in the main `pyproject.toml`:

- `pytest` - Test runner
- `pytest-asyncio` - Async test support
- `pytest-cov` - Coverage reporting

### Environment Setup

Most tests use mocked metrics providers and don't require external services. For tests that need real services:

```bash
# Set observability configuration (optional - tests use defaults)
export OTEL_EXPORTER_OTLP_ENABLED=false
export SUBPROCESS_METRICS_SCRAPE_INTERVAL=30
```

## Key Observability Modules

The tests validate these core modules:

### Metrics Recording

- `src/datahub_integrations/observability/bot_metrics.py` - Bot operation metrics
- `src/datahub_integrations/observability/cost.py` - LLM cost calculation and tracking
- `src/datahub_integrations/observability/event_processing_metrics.py` - Action metrics

### Configuration & Setup

- `src/datahub_integrations/observability/config.py` - Centralized configuration
- `src/datahub_integrations/observability/otel_config.py` - OpenTelemetry initialization

### Instrumentation Helpers

- `src/datahub_integrations/observability/decorators.py` - Metric decorators
- `src/datahub_integrations/observability/cost_utils.py` - Cost tracking utilities
- `src/datahub_integrations/observability/metrics_registry.py` - Metric definitions

### Metrics Collection

- `src/datahub_integrations/observability/subprocess_metrics_bridge.py` - Subprocess aggregation
- `src/datahub_integrations/observability/metrics_constants.py` - Metric name constants

## Adding New Tests

When adding new observability features:

1. **Add unit tests** to `tests/unit/observability/` for:

   - Configuration and initialization logic
   - Metric calculation and formatting
   - Helper functions and utilities

2. **Add integration tests** to `tests/observability/` (this directory) for:

   - End-to-end metric recording flows
   - Context manager behavior
   - Integration with ActionStageReport/EventProcessingStats

3. **Add smoke tests** to main DataHub repo for:
   - Real service interactions
   - Actual LLM calls with cost tracking
   - Metrics endpoint validation
   - Dashboard functionality

### Test Patterns

**Unit Test Pattern:**

```python
def test_cost_calculation():
    """Test cost calculation for specific model."""
    calculator = CostCalculator()
    cost = calculator.calculate("anthropic.claude-3-opus",
                                prompt_tokens=1000,
                                completion_tokens=500)
    assert cost > 0
```

**Integration Test Pattern:**

```python
def test_record_bot_metric():
    """Test bot metric recording executes without error."""
    record_slack_api_call(
        api_method="chat.postMessage",
        duration_seconds=0.2,
        success=True
    )
    # Metric recorded successfully
```

## Troubleshooting

### Tests Pass But No Metrics Visible

This is expected for unit tests. OpenTelemetry's global `MeterProvider` is set at import time, and unit tests use mocked providers. To see real metrics:

1. Run the actual service: `uvicorn datahub_integrations.server:app`
2. Trigger operations via API calls
3. Check `/metrics` endpoint for Prometheus output

### Import Errors

```bash
# Ensure you're in the correct directory and venv is activated
cd datahub-integrations-service
source venv/bin/activate
pytest tests/observability/ -v
```

### Mock vs Real Metrics

Unit tests validate that metric recording functions execute without errors, but don't verify actual Prometheus output. For end-to-end validation, use:

- Integration tests (this directory) - Validate recording logic
- Smoke tests (main repo) - Validate actual metrics output

## Contributing

When modifying observability code:

1. ✅ Run relevant unit tests: `pytest tests/unit/observability/ -v`
2. ✅ Run integration tests: `pytest tests/observability/ -v`
3. ✅ Check type annotations: `mypy src/datahub_integrations/observability/`
4. ✅ Run linting: `ruff check src/datahub_integrations/observability/`
5. ✅ Update this README if adding new test categories

See existing test files for patterns and examples.
