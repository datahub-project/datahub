# RDF Source Integration Tests

This directory contains integration tests for the RDF ingestion source.

## Test Coverage

The integration tests cover:

1. **Basic Ingestion**

   - Simple glossary term extraction
   - Glossary terms with relationships (broader/narrower)
   - Glossary terms with domain hierarchy

2. **RDF Formats**

   - Turtle (TTL)
   - RDF/XML
   - JSON-LD

3. **Features**

   - Recursive directory ingestion
   - Selective export (`export_only` filter)
   - Skip export (`skip_export` filter)
   - Stateful ingestion with stale entity removal

4. **Error Handling**
   - Missing file errors
   - Malformed RDF errors

## Prerequisites

### Standard DataHub Setup (Recommended)

DataHub uses a virtual environment for development. Follow these steps:

```bash
# 1. From repository root, set up the development environment
./gradlew :metadata-ingestion:installDev

# 2. From metadata-ingestion directory, activate the virtual environment
cd metadata-ingestion
source venv/bin/activate

# 3. Install minimal test dependencies (if not already installed)
pip install pytest-docker freezegun "deepdiff!=8.0.0" PyYAML
```

### Alternative: Manual Setup

If you're not using the standard DataHub setup:

```bash
# From metadata-ingestion directory
pip install pytest-docker freezegun "deepdiff!=8.0.0" PyYAML
```

**Note**: Always activate the virtual environment (`source venv/bin/activate`) before running tests if you're using the standard DataHub setup.

## Running Tests

**Important**: Make sure the virtual environment is activated (`source venv/bin/activate`) before running tests.

### Quick Tips for Daily Use

**Yes, you need to activate the venv in each new terminal session.** Here are some ways to make it easier:

1. **Auto-activate when entering the directory** (add to your `~/.zshrc` or `~/.bashrc`):

   ```bash
   # Auto-activate venv when entering metadata-ingestion directory
   cd() {
     builtin cd "$@"
     if [[ "$PWD" == */metadata-ingestion ]] && [[ -f venv/bin/activate ]]; then
       source venv/bin/activate
     fi
   }
   ```

2. **Create an alias** (add to your `~/.zshrc`):

   ```bash
   alias datahub-dev='cd /Users/stephengoldbaum/Code/datahub/metadata-ingestion && source venv/bin/activate'
   ```

   Then just run `datahub-dev` to jump to the directory and activate.

3. **Use the venv's Python directly** (no activation needed):

   ```bash
   ./venv/bin/pytest tests/integration/rdf/test_rdf_source.py -v
   ```

4. **Use direnv** (if you have it installed):
   Create `.envrc` in `metadata-ingestion/`:
   ```bash
   source venv/bin/activate
   ```
   Then `direnv allow` - it auto-activates when you `cd` into the directory.

### Run all RDF integration tests:

```bash
# With venv activated
pytest tests/integration/rdf/test_rdf_source.py -v

# Or without venv activation (using venv's Python directly)
./venv/bin/pytest tests/integration/rdf/test_rdf_source.py -v
```

### Run a specific test:

```bash
pytest tests/integration/rdf/test_rdf_source.py::test_simple_glossary_ingestion -v
```

### Update golden files:

After making changes to the RDF source that affect output, update the golden files:

```bash
pytest tests/integration/rdf/test_rdf_source.py --update-golden-files
```

## Golden Files

Golden files contain the expected output for each test. They are JSON files containing MCPs (Metadata Change Proposals) in DataHub's standard format.

The golden files are currently placeholder empty arrays (`[]`). To generate them:

1. Run the tests with `--update-golden-files` flag
2. Review the generated files to ensure they match expected output
3. Commit the updated golden files

## Test Structure

- **Fixtures**: Create temporary RDF files for testing
- **Tests**: Run the full ingestion pipeline and verify output
- **Golden Files**: Expected output for comparison

Each test:

1. Creates a temporary RDF file with test data
2. Runs the ingestion pipeline
3. Compares output against golden file
4. Verifies no errors occurred (unless testing error scenarios)
