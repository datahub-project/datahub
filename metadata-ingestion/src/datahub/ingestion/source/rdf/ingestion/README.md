# RDF DataHub Ingestion Source

This module implements a DataHub ingestion source plugin for RDF, allowing RDF ontologies to be ingested using DataHub's native ingestion framework.

## Architecture

The ingestion source follows DataHub's Source API pattern:

```
RDF Files → RDFSource → MetadataWorkUnits → DataHub
```

### Key Components

1. **RDFSourceConfig** - Pydantic configuration model

   - Defines all configuration parameters
   - Validates input values
   - Provides configuration for RDF source

2. **RDFSource** - Main source class

   - Implements `datahub.ingestion.api.source.Source`
   - Decorated with `@config_class`, `@platform_name`, `@support_status`
   - Yields `MetadataWorkUnit` objects containing MCPs

3. **RDFSourceReport** - Ingestion report

   - Tracks statistics (files processed, entities emitted, etc.)
   - Reports errors and warnings
   - Extends `SourceReport` from DataHub SDK

4. **DataHubIngestionTarget** - Internal target adapter
   - Implements `TargetInterface` from RDF core
   - Converts DataHub AST to MetadataWorkUnits
   - Bridges RDF transpiler with DataHub ingestion framework

## How It Works

1. **Configuration** - DataHub parses recipe YAML and creates `RDFSourceConfig`

2. **Initialization** - `RDFSource` is created with config and pipeline context

3. **Work Unit Generation** - `get_workunits()` is called:

   - Creates RDF source (file, folder, URL) using `SourceFactory`
   - Creates `DataHubIngestionTarget` to collect work units
   - Creates transpiler with configuration
   - Executes orchestrator pipeline
   - Yields collected work units

4. **MCP Generation** - `DataHubIngestionTarget`:

   - Receives DataHub AST from transpiler
   - Generates MCPs directly from entity MCP builders
   - Wraps MCPs in `MetadataWorkUnit` objects
   - Returns work units to source

5. **Ingestion** - DataHub ingestion framework:
   - Receives work units from source
   - Applies transformers (if configured)
   - Sends to DataHub GMS via sink

## Plugin Registration

The source is registered as a DataHub plugin in `pyproject.toml`:

```toml
[project.entry-points."datahub.ingestion.source.plugins"]
rdf = "rdf.ingestion:RDFSource"
```

This makes it available as `type: rdf` in recipe files.

## Configuration Parameters

See `RDFSourceConfig` class for all available parameters. Key parameters:

- `source` - RDF source (file, folder, URL, comma-separated files)
- `environment` - DataHub environment (PROD, DEV, TEST)
- `format` - RDF format (turtle, xml, n3, etc.) - auto-detected if not specified
- `dialect` - RDF dialect (default, fibo, generic) - auto-detected if not specified
- `export_only` - Export only specified entity types
- `skip_export` - Skip specified entity types

## Example Recipe

```yaml
source:
  type: rdf
  config:
    source: examples/bcbs239/
    environment: PROD
    export_only:
      - glossary

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

## Development

### Testing the Source

```bash
# Install in development mode
pip install -e .

# Verify plugin is registered
datahub check plugins

# Run with a recipe
datahub ingest -c examples/recipe_basic.yml --dry-run
```

### Adding New Configuration Parameters

1. Add field to `RDFSourceConfig` class
2. Add validator if needed (using pydantic's `@validator`)
3. Use parameter in `_create_source()`, `_create_query()`, or `_create_transpiler()`
4. Update example recipes
5. Update documentation

### Debugging

Enable debug logging:

```bash
datahub ingest -c examples/recipe_basic.yml --debug
```

Check logs in the source:

```python
import logging
logger = logging.getLogger(__name__)
logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
```

## Design Decisions

### Why DataHubIngestionTarget?

The `DataHubIngestionTarget` class bridges the RDF core (which expects a `TargetInterface`) with DataHub's ingestion framework (which expects work units). This allows us to:

1. Reuse the entire RDF transpiler pipeline
2. Maintain separation of concerns
3. Avoid duplicating MCP generation logic
4. Keep the ingestion source thin and focused

### MCP Generation

MCPs are generated directly by entity MCP builders, ensuring: 2. Single source of truth for MCP generation 3. Easier maintenance (fix once, works everywhere)

### Configuration Parameters

The configuration parameters provide: 2. Convert to recipes for production 3. Use the same parameters in both interfaces

## Future Enhancements

Potential improvements for future development:

1. **Incremental Ingestion** - Track last modified times, only process changed files
2. **Parallel Processing** - Process multiple files in parallel
3. **Caching** - Cache parsed RDF graphs to avoid re-parsing
4. **Custom Transformers** - RDF-specific transformers for common operations
5. **Source Status** - Report detailed statistics about processed entities
6. **Validation** - Validate RDF before ingestion with detailed error reports

## Related Files

- `src/rdf/core/orchestrator.py` - Pipeline orchestrator
- `src/rdf/core/transpiler.py` - 3-phase transpiler
- `src/rdf/entities/*/mcp_builder.py` - Entity-specific MCP builders
- `examples/RECIPES.md` - Recipe documentation
- `CLAUDE.md` - Overall architecture documentation
