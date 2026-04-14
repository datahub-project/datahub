import { afterEach, describe, expect, it, vi } from 'vitest';

import { FieldType, FilterRecipeField, FilterRule } from '@app/ingestV2/source/builder/RecipeForm/common';
import { Filter } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/types';
import {
    convertFiltersToFieldValues,
    filterOutUnsupportedFields,
    getEmptyFilter,
    getInitialFilters,
    getOptionsForTypeSelect,
    getSubtypeOptions,
} from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/utils';

// Mock the uuid function to return predictable values for testing
vi.mock('uuid', async () => {
    const actual = await vi.importActual('uuid');
    return {
        ...actual,
        v4: vi.fn(() => 'mocked-uuid'),
    };
});

describe('Filters utils', () => {
    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('getEmptyFilter', () => {
        it('should return a filter with default values', () => {
            const result = getEmptyFilter();

            expect(result).toEqual({
                key: 'mocked-uuid',
                rule: undefined,
                subtype: undefined,
                value: '',
            });
        });
    });

    describe('getInitialFilters', () => {
        const mockFields: FilterRecipeField[] = [
            {
                name: 'database_pattern.allow',
                label: 'Allow Patterns',
                helper: 'Include specific Databases',
                tooltip: 'Tooltip',
                placeholder: 'database_name',
                type: FieldType.LIST,
                buttonLabel: 'Add pattern',
                fieldPath: 'source.config.database_pattern.allow',
                rules: null,
                section: 'Databases',
                filteringResource: 'Database',
                rule: FilterRule.INCLUDE,
                setValueOnRecipeOverride: vi.fn(),
            },
            {
                name: 'database_pattern.deny',
                label: 'Deny Patterns',
                helper: 'Exclude specific Databases',
                tooltip: 'Tooltip',
                placeholder: 'database_name',
                type: FieldType.LIST,
                buttonLabel: 'Add pattern',
                fieldPath: 'source.config.database_pattern.deny',
                rules: null,
                section: 'Databases',
                filteringResource: 'Database',
                rule: FilterRule.EXCLUDE,
                setValueOnRecipeOverride: vi.fn(),
            },
        ];

        it('should return filters from recipe values when present', () => {
            const recipe = `
source:
  config:
    database_pattern:
      allow: ["db1", "db2"]
      deny: ["db3"]
            `.trim();

            const result = getInitialFilters(mockFields, recipe);

            expect(result).toHaveLength(3); // db1, db2, db3
            expect(result).toContainEqual({
                key: 'mocked-uuid',
                rule: FilterRule.INCLUDE,
                subtype: 'Database',
                value: 'db1',
            });
            expect(result).toContainEqual({
                key: 'mocked-uuid',
                rule: FilterRule.INCLUDE,
                subtype: 'Database',
                value: 'db2',
            });
            expect(result).toContainEqual({
                key: 'mocked-uuid',
                rule: FilterRule.EXCLUDE,
                subtype: 'Database',
                value: 'db3',
            });
        });

        it('should return a single empty filter when no values are found in recipe', () => {
            const recipe = `
source:
  config:
    other_config: true
            `.trim();

            const result = getInitialFilters(mockFields, recipe);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                key: 'mocked-uuid',
                rule: undefined,
                subtype: undefined,
                value: '',
            });
        });

        it('should return empty filter when getValuesFromRecipe throws an error', () => {
            const invalidRecipe = '{ invalid yaml';

            const result = getInitialFilters(mockFields, invalidRecipe);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                key: 'mocked-uuid',
                rule: undefined,
                subtype: undefined,
                value: '',
            });
        });
    });

    describe('getOptionsForTypeSelect', () => {
        it('should return include and exclude options', () => {
            const result = getOptionsForTypeSelect();

            expect(result).toEqual([
                { label: 'Include', value: 'include' },
                { label: 'Exclude', value: 'exclude' },
            ]);
        });
    });

    describe('getSubtypeOptions', () => {
        it('should return unique subtype options from fields', () => {
            const fields: FilterRecipeField[] = [
                {
                    name: 'field1',
                    label: 'Field 1',
                    helper: 'Helper 1',
                    tooltip: 'Tooltip 1',
                    placeholder: 'placeholder1',
                    type: FieldType.LIST,
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'field2',
                    label: 'Field 2',
                    helper: 'Helper 2',
                    tooltip: 'Tooltip 2',
                    placeholder: 'placeholder2',
                    type: FieldType.LIST,
                    fieldPath: 'path2',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.EXCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'field3',
                    label: 'Field 3',
                    helper: 'Helper 3',
                    tooltip: 'Tooltip 3',
                    placeholder: 'placeholder3',
                    type: FieldType.LIST,
                    fieldPath: 'path3',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'field4',
                    label: 'Field 4',
                    helper: 'Helper 4',
                    tooltip: 'Tooltip 4',
                    placeholder: 'placeholder4',
                    type: FieldType.LIST,
                    fieldPath: 'path4',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const result = getSubtypeOptions(fields);

            expect(result).toEqual([
                { label: 'Database', value: 'Database' },
                { label: 'Schema', value: 'Schema' },
            ]);
        });

        it('should return empty array when no fields provided', () => {
            const result = getSubtypeOptions([]);

            expect(result).toEqual([]);
        });
    });

    describe('filterOutUnsupportedFields', () => {
        it('should return all fields when they are all supported (LIST type)', () => {
            const fields: FilterRecipeField[] = [
                {
                    name: 'field1',
                    label: 'Field 1',
                    helper: 'Helper 1',
                    tooltip: 'Tooltip 1',
                    placeholder: 'placeholder1',
                    type: FieldType.LIST,
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'field2',
                    label: 'Field 2',
                    helper: 'Helper 2',
                    tooltip: 'Tooltip 2',
                    placeholder: 'placeholder2',
                    type: FieldType.LIST,
                    fieldPath: 'path2',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.EXCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const consoleSpy = vi.spyOn(console, 'warn');

            const result = filterOutUnsupportedFields(fields);

            expect(result).toEqual(fields);
            expect(consoleSpy).not.toHaveBeenCalled();

            consoleSpy.mockClear();
        });

        it('should return only LIST type fields when there are unsupported types', () => {
            const fields: FilterRecipeField[] = [
                {
                    name: 'field1',
                    label: 'Field 1',
                    helper: 'Helper 1',
                    tooltip: 'Tooltip 1',
                    placeholder: 'placeholder1',
                    type: FieldType.LIST,
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'field2',
                    label: 'Field 2',
                    helper: 'Helper 2',
                    tooltip: 'Tooltip 2',
                    placeholder: 'placeholder2',
                    type: FieldType.TEXT, // Unsupported type
                    fieldPath: 'path2',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.EXCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const consoleSpy = vi.spyOn(console, 'warn');

            const result = filterOutUnsupportedFields(fields);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe(FieldType.LIST);

            expect(consoleSpy).toHaveBeenCalledWith(
                'Some fields have unsupported type:',
                expect.arrayContaining([expect.objectContaining({ type: FieldType.TEXT })]),
            );

            consoleSpy.mockClear();
        });

        it('should return empty array when all fields are unsupported', () => {
            const fields: FilterRecipeField[] = [
                {
                    name: 'field1',
                    label: 'Field 1',
                    helper: 'Helper 1',
                    tooltip: 'Tooltip 1',
                    placeholder: 'placeholder1',
                    type: FieldType.TEXT, // Unsupported type
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'field2',
                    label: 'Field 2',
                    helper: 'Helper 2',
                    tooltip: 'Tooltip 2',
                    placeholder: 'placeholder2',
                    type: FieldType.BOOLEAN, // Unsupported type
                    fieldPath: 'path2',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.EXCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const consoleSpy = vi.spyOn(console, 'warn');

            const result = filterOutUnsupportedFields(fields);

            expect(result).toEqual([]);
            expect(consoleSpy).toHaveBeenCalled();

            consoleSpy.mockClear();
        });
    });

    describe('convertFiltersToFieldValues', () => {
        it('should map filters to field values correctly', () => {
            const filters: Filter[] = [
                { key: 'key1', rule: FilterRule.INCLUDE, subtype: 'Database', value: 'db1' },
                { key: 'key2', rule: FilterRule.INCLUDE, subtype: 'Database', value: 'db2' },
                { key: 'key3', rule: FilterRule.EXCLUDE, subtype: 'Schema', value: 'schema1' },
            ];

            const fields: FilterRecipeField[] = [
                {
                    name: 'database_pattern.allow',
                    label: 'Allow Patterns',
                    helper: 'Helper',
                    tooltip: 'Tooltip',
                    placeholder: 'placeholder',
                    type: FieldType.LIST,
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'database_pattern.deny',
                    label: 'Deny Patterns',
                    helper: 'Helper',
                    tooltip: 'Tooltip',
                    placeholder: 'placeholder',
                    type: FieldType.LIST,
                    fieldPath: 'path2',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.EXCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const result = convertFiltersToFieldValues(filters, fields);

            expect(result).toEqual({
                'database_pattern.allow': ['db1', 'db2'],
                'database_pattern.deny': ['schema1'],
            });
        });

        it('should return empty arrays for fields with no matching filters', () => {
            const filters: Filter[] = [{ key: 'key1', rule: FilterRule.INCLUDE, subtype: 'Database', value: 'db1' }];

            const fields: FilterRecipeField[] = [
                {
                    name: 'database_pattern.allow',
                    label: 'Allow Patterns',
                    helper: 'Helper',
                    tooltip: 'Tooltip',
                    placeholder: 'placeholder',
                    type: FieldType.LIST,
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
                {
                    name: 'schema_pattern.allow',
                    label: 'Allow Patterns',
                    helper: 'Helper',
                    tooltip: 'Tooltip',
                    placeholder: 'placeholder',
                    type: FieldType.LIST,
                    fieldPath: 'path2',
                    rules: null,
                    section: 'Schema',
                    filteringResource: 'Schema',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const result = convertFiltersToFieldValues(filters, fields);

            expect(result).toEqual({
                'database_pattern.allow': ['db1'],
                'schema_pattern.allow': [],
            });
        });

        it('should return empty object when no fields provided', () => {
            const filters: Filter[] = [{ key: 'key1', rule: FilterRule.INCLUDE, subtype: 'Database', value: 'db1' }];

            const result = convertFiltersToFieldValues(filters, []);

            expect(result).toEqual({});
        });

        it('should return empty object when no filters provided', () => {
            const fields: FilterRecipeField[] = [
                {
                    name: 'database_pattern.allow',
                    label: 'Allow Patterns',
                    helper: 'Helper',
                    tooltip: 'Tooltip',
                    placeholder: 'placeholder',
                    type: FieldType.LIST,
                    fieldPath: 'path1',
                    rules: null,
                    section: 'Database',
                    filteringResource: 'Database',
                    rule: FilterRule.INCLUDE,
                    setValueOnRecipeOverride: vi.fn(),
                },
            ];

            const result = convertFiltersToFieldValues([], fields);

            expect(result).toEqual({
                'database_pattern.allow': [],
            });
        });
    });
});
