# RDF to DataHub Transpiler Architecture

## Overview

This document describes the new transpiler architecture that provides clean separation of concerns for RDF to DataHub conversion. The architecture follows a three-phase transpiler pattern similar to how compilers work.

## Architecture Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RDF Graph     │───▶│   RDF AST       │───▶│  DataHub AST    │───▶│  DataHub SDK    │
│   (Input)       │    │   (Internal)    │    │   (Internal)    │    │   (Output)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
        │                        │                        │                        │
        │                        │                        │                        │
        ▼                        ▼                        ▼                        ▼
   RDFToASTConverter    ASTToDataHubConverter    OutputStrategy         DataHub API
```

## Three Phases

### Phase 1: RDF Graph → RDF AST

**File:** `rdf_graph_to_rdf_ast_converter.py`
**Purpose:** Pure RDF parsing and extraction
**Input:** RDFLib Graph
**Output:** Internal RDF AST representation

**Key Classes:**

- `RDFToASTConverter`: Converts RDF graphs to internal AST
- `RDFGraph`: Internal representation of RDF data
- `RDFDataset`, `RDFGlossaryTerm`, `RDFStructuredProperty`: Entity representations

**Responsibilities:**

- Parse RDF triples into structured data
- Extract datasets, glossary terms, and properties
- Identify relationships between entities
- Handle various RDF patterns (SKOS, OWL, DCAT, etc.)

### Phase 2: RDF AST → DataHub AST

**File:** `rdf_ast_to_datahub_ast_converter.py`
**Purpose:** DataHub object preparation and URN generation
**Input:** RDF AST representation
**Output:** DataHub-specific AST representation

**Key Classes:**

- `ASTToDataHubConverter`: Converts RDF AST to DataHub AST
- `DataHubGraph`: Internal DataHub representation
- `DataHubDataset`, `DataHubGlossaryTerm`, `DataHubStructuredProperty`: DataHub entity representations

**Responsibilities:**

- Generate DataHub URNs
- Convert RDF types to DataHub types
- Prepare DataHub-specific metadata
- Handle DataHub naming conventions

### Phase 3: DataHub AST → Output

**File:** `output_strategies.py`
**Purpose:** Execute DataHub operations via strategy pattern
**Input:** DataHub AST representation
**Output:** Execution results

**Key Classes:**

- `OutputStrategy`: Abstract base class for output strategies
- `PrettyPrintStrategy`: Externalizes DataHub AST in human-readable format
- `LiveDataHubStrategy`: Actual DataHub API operations
- `FileOutputStrategy`: File-based output

**Responsibilities:**

- Execute DataHub operations
- Handle validation and error reporting
- Provide different output modes (pretty print, live, file)
- Externalize DataHub AST for inspection

## Main Orchestrator

**File:** `transpiler.py`
**Purpose:** Coordinate the three phases
**Key Class:** `RDFToDataHubTranspiler`

**Usage Examples:**

```python
# Create transpiler and target using polymorphic pattern
from rdf.core.transpiler import RDFToDataHubTranspiler
from rdf.core.target_factory import TargetFactory

transpiler = RDFToDataHubTranspiler("PROD", datahub_client)

# Pretty print target
target = TargetFactory.create_pretty_print_target()
datahub_ast = transpiler.get_datahub_ast(rdf_graph)
results = target.execute(datahub_ast)

# Live DataHub target
target = TargetFactory.create_datahub_target(datahub_client)
datahub_ast = transpiler.get_datahub_ast(rdf_graph)
results = target.execute(datahub_ast)

# Custom output strategy
results = transpiler.transpile(rdf_graph, CustomOutputStrategy())

# Phase-by-phase (for debugging)
rdf_ast = transpiler.get_rdf_ast(rdf_graph)
datahub_ast = transpiler.get_datahub_ast(rdf_graph)
results = strategy.execute(datahub_ast)
```

## Benefits

### 1. **Clean Separation of Concerns**

- RDF parsing logic is separate from DataHub logic
- Each phase has a single responsibility
- Easy to understand and maintain

### 2. **Modular Testing**

- Each phase can be tested independently
- Easy to isolate issues
- Clear test boundaries

### 3. **Flexible Output**

- Multiple output strategies (pretty print, live, file)
- Easy to add new output formats
- Strategy pattern enables different execution modes

### 4. **Debugging and Development**

- Can inspect intermediate ASTs
- Phase-by-phase execution for debugging
- Clear error boundaries
- Pretty print externalizes DataHub AST for inspection

### 5. **Reusability**

- DataHub AST can be used for different outputs
- RDF AST can be used for different targets
- Components are loosely coupled

## Testing Strategy

### Phase 1 Tests: RDF → RDF AST

```python
def test_rdf_to_ast_conversion():
    rdf_graph = load_test_rdf()
    ast = RDFToASTConverter().convert(rdf_graph)

    assert len(ast.datasets) == 3
    assert ast.datasets[0].name == "CustomerData"
    assert len(ast.glossary_terms) == 5
```

### Phase 2 Tests: RDF AST → DataHub AST

```python
def test_ast_to_datahub_conversion():
    rdf_ast = create_test_rdf_ast()
    datahub_ast = ASTToDataHubConverter().convert(rdf_ast)

    assert datahub_ast.datasets[0].urn.startswith("urn:li:dataset:")
    assert isinstance(datahub_ast.datasets[0].properties, DatasetPropertiesClass)
```

### Phase 3 Tests: DataHub AST → Output

```python
def test_pretty_print_output():
    datahub_ast = create_test_datahub_ast()
    strategy = PrettyPrintStrategy()
    result = strategy.execute(datahub_ast)

    assert result['strategy'] == 'pretty_print'
    assert 'pretty_output' in result
    assert 'Test Dataset' in result['pretty_output']
```

## Migration from Current Architecture

The current `DataHubExporter` class mixes concerns and should be refactored to use this new architecture:

**Before (Mixed Concerns):**

```python
class DataHubExporter:
    def export_datasets_with_properties(self, datasets_data):
        # RDF interpretation + DataHub object creation + URN generation
        pass
```

**After (Clean Separation):**

```python
# Phase 1: RDF → RDF AST
rdf_ast = RDFToASTConverter().convert(rdf_graph)

# Phase 2: RDF AST → DataHub AST
datahub_ast = ASTToDataHubConverter().convert(rdf_ast)

# Phase 3: DataHub AST → Output
results = PrettyPrintStrategy().execute(datahub_ast)
```

## Files Created

1. **`ast.py`** - Internal AST data structures
2. **`rdf_graph_to_rdf_ast_converter.py`** - Phase 1 converter
3. **`rdf_ast_to_datahub_ast_converter.py`** - Phase 2 converter
4. **`output_strategies.py`** - Phase 3 strategies
5. **`transpiler.py`** - Main orchestrator
6. **`transpiler_example.py`** - Usage examples
7. **`test_transpiler_architecture.py`** - Test examples

## Next Steps

1. **Integrate with existing codebase** - Update current classes to use new architecture
2. **Add comprehensive tests** - Create full test suite for each phase
3. **Performance optimization** - Optimize each phase for large datasets
4. **Error handling** - Add robust error handling and recovery
5. **Documentation** - Add detailed API documentation

This architecture provides a solid foundation for maintainable, testable, and extensible RDF to DataHub conversion.
