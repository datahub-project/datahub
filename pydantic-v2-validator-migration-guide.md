# Pydantic V1 → V2 Validator Migration Guide for DataHub

You are tasked with completing the migration of Pydantic validators from V1 to V2 in DataHub's metadata-ingestion codebase. This migration must strictly follow the official Pydantic migration guide: https://docs.pydantic.dev/latest/migration/#changes-to-validators

## Context & Background

DataHub has already migrated from Pydantic V1 to V2, with most validators converted from `@pydantic.validator()` to `@field_validator()` and `@model_validator()`. However, two critical issues remain:

1. **Missing Type Annotations**: Many validators lack proper type annotations for Pydantic V2
2. **Incomplete `always=True` Migration**: Fields that had `@pydantic.validator("field", always=True)` need `Field(validate_default=True)` to preserve validation behavior on default values

## Official Migration Blueprints

### Field Validator Migration (per Pydantic docs)

**V1 → V2 Syntax Changes:**
```python
# V1
@pydantic.validator('field_name')
def validate_field(cls, v):
    return v

# V2
@field_validator('field_name')
@classmethod
def validate_field(cls, v: FieldType) -> FieldType:
    return v
```

**Mode Handling (Critical):**
- **V1 `pre=True`** → **V2 `mode='before'`**: Receives raw input before parsing
- **V1 default (pre=False)** → **V2 `mode='after'` or no mode**: Receives parsed values after type conversion
- **V2 default mode is 'after'** (equivalent to V1's pre=False)

### Model Validator Migration

```python
# V1
@pydantic.root_validator
def validate_model(cls, values):
    return values

# V2
@model_validator(mode='before')
@classmethod
def validate_model(cls, values: Any) -> Any:
    return values
```

### Type Annotation Patterns (Following Official Guidance)

**Field Validators:**
- `mode='before'`: `cls, v: Any → Any` (raw input, any type possible)
- `mode='after'` or no mode: `cls, v: FieldType → FieldType` (parsed value, known type)
- With ValidationInfo: Add `info: ValidationInfo` parameter

**Model Validators:**
- `mode='before'`: `cls, values: Any → Any` (raw dict input)
- `mode='after'`: `self → Self` (model instance, use `Self` or `"ClassName"`)

### Critical Migration: `always=True` → `Field(validate_default=True)`

Per Pydantic migration guide, `always=True` behavior is replaced by `Field(validate_default=True)`:

```python
# V1
field_name: Optional[str] = None

@pydantic.validator('field_name', always=True)
def validate_field(cls, v):
    return v or "default"

# V2 - TWO CHANGES REQUIRED:
field_name: Optional[str] = Field(default=None, validate_default=True)  # 1. Add Field with validate_default

@field_validator('field_name')  # 2. Update validator syntax
@classmethod
def validate_field(cls, v: Optional[str]) -> Optional[str]:
    return v or "default"
```

## Methodical Approach (Aligned with Official Migration)

### Phase 1: Inventory Current State
```bash
# Find validators missing type annotations
../gradlew :metadata-ingestion:lint | grep "TID251"
```

### Phase 2: Apply Official Migration Patterns
For each validator, follow this exact sequence:

1. **Identify original V1 pattern** (from git history or comments)
2. **Apply official V2 syntax** per migration guide
3. **Preserve mode behavior**:
   - If validator had `pre=True` → add `mode='before'`
   - If validator had no `pre` or `pre=False` → use `mode='after'` or no mode
   - **NEVER guess modes** - check existing behavior
4. **Add correct type annotations** based on mode
5. **Handle `always=True`** → `Field(validate_default=True)`

### Phase 3: Validate Against Official Examples

Cross-reference each change against official Pydantic migration examples:
- https://docs.pydantic.dev/latest/migration/#changes-to-validators
- https://docs.pydantic.dev/latest/api/functional_validators/

## Type Annotation Standards (Per Pydantic V2)

### Import Requirements
```python
from typing import Any
from pydantic import field_validator, model_validator, ValidationInfo
```

### Field Validator Signatures
```python
# Before mode (raw input)
@field_validator('field', mode='before')
@classmethod
def validate_field(cls, v: Any) -> Any:
    ...

# After mode (parsed value)
@field_validator('field')  # mode='after' is default
@classmethod
def validate_field(cls, v: FieldType) -> FieldType:
    ...

# With validation info
@field_validator('field', mode='before')
@classmethod
def validate_field(cls, v: Any, info: ValidationInfo) -> Any:
    ...
```

### Model Validator Signatures
```python
# Before mode (dict input)
@model_validator(mode='before')
@classmethod
def validate_model(cls, values: Any) -> Any:
    ...

# After mode (model instance)
@model_validator(mode='after')
def validate_model(self) -> Self:  # or "ClassName"
    ...
```

## Testing Strategy (Migration Validation)

### Behavioral Equivalence Tests
Ensure V2 validators behave identically to V1:

```bash
# After each batch
../gradlew :metadata-ingestion:testQuick

# Validate type checking
../gradlew :metadata-ingestion:lint
```

### Critical Test Cases
1. **Default value validation**: Test `Field(validate_default=True)` works
2. **Mode preservation**: Verify `before` vs `after` behavior unchanged
3. **Type safety**: Ensure no runtime type errors
4. **Edge cases**: None values, empty strings, invalid inputs

## Key Compliance Points

### Must Follow Official Migration Guide
1. **Use exact V2 syntax** from Pydantic docs
2. **Map V1 modes correctly**: `pre=True` → `mode='before'`, `pre=False` → `mode='after'`
3. **Apply `always=True` migration**: → `Field(validate_default=True)`
4. **Use proper type annotations** as documented

### Avoid Common Migration Mistakes
1. **Don't change validator behavior** - only syntax and types
2. **Don't guess modes** - preserve existing V1 behavior
3. **Don't skip `always=True` migration** - this changes validation behavior
4. **Don't use deprecated patterns** - follow current V2 best practices

## Success Criteria

- [ ] All validators follow official V2 syntax patterns
- [ ] Type annotations match Pydantic V2 standards
- [ ] All `always=True` cases migrated to `Field(validate_default=True)`
- [ ] Zero TID251 linting violations
- [ ] All existing tests pass (behavior preserved)
- [ ] Code follows official migration guide examples

## Reference Links

- **Primary**: https://docs.pydantic.dev/latest/migration/#changes-to-validators
- **Validator API**: https://docs.pydantic.dev/latest/api/functional_validators/
- **Field API**: https://docs.pydantic.dev/latest/api/fields/

This migration is critical for DataHub's type safety. Follow the official Pydantic migration guide exactly - do not deviate from documented patterns.