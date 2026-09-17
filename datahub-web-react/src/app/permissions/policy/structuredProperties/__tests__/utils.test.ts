import { describe, expect, it } from 'vitest';

import {
    StructuredPropertyDefinition,
    StructuredPropertyValue,
    buildDefinitionsMap,
    buildPropertyOptionsMap,
    getEntityTypes,
    getValueOptions,
    getValueType,
    hasIncompleteStructuredProperties,
} from '@app/permissions/policy/structuredProperties/utils';

describe('StructuredProperties Utils', () => {
    describe('hasIncompleteStructuredProperties', () => {
        it('returns false for empty array', () => {
            expect(hasIncompleteStructuredProperties([])).toBe(false);
        });

        it('returns false when all properties have URN and values', () => {
            const properties: StructuredPropertyValue[] = [
                { propertyUrn: 'urn:li:structuredProperty:prop1', values: ['value1'] },
                { propertyUrn: 'urn:li:structuredProperty:prop2', values: ['value2', 'value3'] },
            ];
            expect(hasIncompleteStructuredProperties(properties)).toBe(false);
        });

        it('returns false when propertyUrn is empty string', () => {
            const properties: StructuredPropertyValue[] = [{ propertyUrn: '', values: ['value1'] }];
            expect(hasIncompleteStructuredProperties(properties)).toBe(false);
        });

        it('returns true when propertyUrn exists but values are empty', () => {
            const properties: StructuredPropertyValue[] = [
                { propertyUrn: 'urn:li:structuredProperty:prop1', values: [] },
            ];
            expect(hasIncompleteStructuredProperties(properties)).toBe(true);
        });

        it('returns true when propertyUrn exists but values is not an array', () => {
            const properties = [{ propertyUrn: 'urn:li:structuredProperty:prop1', values: undefined }] as any;
            expect(hasIncompleteStructuredProperties(properties)).toBe(true);
        });

        it('returns true when mixed complete and incomplete properties', () => {
            const properties: StructuredPropertyValue[] = [
                { propertyUrn: 'urn:li:structuredProperty:prop1', values: ['value1'] },
                { propertyUrn: 'urn:li:structuredProperty:prop2', values: [] },
            ];
            expect(hasIncompleteStructuredProperties(properties)).toBe(true);
        });
    });

    describe('getValueType', () => {
        it('returns undefined for undefined definition', () => {
            expect(getValueType(undefined)).toBeUndefined();
        });

        it('returns undefined when no valueType', () => {
            const def: StructuredPropertyDefinition = { urn: 'urn:prop1' };
            expect(getValueType(def)).toBeUndefined();
        });

        it('returns value type URN', () => {
            const def: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: { valueType: { urn: 'urn:type:string' } },
            };
            expect(getValueType(def)).toBe('urn:type:string');
        });
    });

    describe('getValueOptions', () => {
        it('returns empty array for undefined definition', () => {
            expect(getValueOptions(undefined)).toEqual([]);
        });

        it('returns empty array when no allowedValues', () => {
            const def: StructuredPropertyDefinition = { urn: 'urn:prop1' };
            expect(getValueOptions(def)).toEqual([]);
        });

        it('returns allowed values', () => {
            const allowedValues = [
                { value: { stringValue: 'option1' }, description: 'Option 1' },
                { value: { stringValue: 'option2' }, description: 'Option 2' },
            ];
            const def: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: { allowedValues },
            };
            expect(getValueOptions(def)).toEqual(allowedValues);
        });
    });

    describe('getEntityTypes', () => {
        it('returns empty array for undefined definition', () => {
            expect(getEntityTypes(undefined)).toEqual([]);
        });

        it('returns empty array when valueType is not URN_TYPE_URN', () => {
            const def: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: { valueType: { urn: 'urn:type:string' } },
            };
            expect(getEntityTypes(def)).toEqual([]);
        });

        it('returns empty array when no typeQualifier', () => {
            const def: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: { valueType: { urn: 'urn:li:type:urn' } },
            };
            expect(getEntityTypes(def)).toEqual([]);
        });

        it('returns entity types from typeQualifier', () => {
            const def: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: {
                    valueType: { urn: 'urn:li:dataType:datahub.urn' },
                    typeQualifier: {
                        allowedTypes: [
                            { type: 'DATASET', info: { type: 'DATASET' } },
                            { type: 'CHART', info: { type: 'CHART' } },
                        ],
                    },
                },
            };
            expect(getEntityTypes(def)).toEqual(['DATASET', 'CHART']);
        });

        it('filters out invalid entity types', () => {
            const def: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: {
                    valueType: { urn: 'urn:li:dataType:datahub.urn' },
                    typeQualifier: {
                        allowedTypes: [
                            { type: 'DATASET', info: { type: 'DATASET' } },
                            { type: undefined, info: {} },
                            { type: 'CHART', info: { type: 'CHART' } },
                        ],
                    },
                },
            };
            expect(getEntityTypes(def)).toEqual(['DATASET', 'CHART']);
        });
    });

    describe('buildPropertyOptionsMap', () => {
        it('builds map from search results', () => {
            const searchResults: StructuredPropertyDefinition[] = [
                {
                    urn: 'urn:prop1',
                    definition: { displayName: 'Property 1' },
                },
                {
                    urn: 'urn:prop2',
                    definition: { displayName: 'Property 2' },
                },
            ];
            const result = buildPropertyOptionsMap(searchResults, undefined, undefined);

            expect(result.size).toBe(2);
            expect(result.get('urn:prop1')).toEqual({
                value: 'urn:prop1',
                label: 'Property 1',
            });
        });

        it('adds selected property data when available', () => {
            const searchResults: StructuredPropertyDefinition[] = [];
            const selectedPropertyData: StructuredPropertyDefinition = {
                urn: 'urn:selected',
                definition: { displayName: 'Selected Property' },
            };
            const result = buildPropertyOptionsMap(searchResults, selectedPropertyData, undefined);

            expect(result.size).toBe(1);
            expect(result.get('urn:selected')).toEqual({
                value: 'urn:selected',
                label: 'Selected Property',
            });
        });

        it('adds placeholder for selected property URN not yet fetched', () => {
            const searchResults: StructuredPropertyDefinition[] = [];
            const result = buildPropertyOptionsMap(searchResults, undefined, 'urn:unfetched');

            expect(result.size).toBe(1);
            expect(result.get('urn:unfetched')).toEqual({
                value: 'urn:unfetched',
                label: 'urn:unfetched',
            });
        });

        it('combines search results, selected data, and placeholder', () => {
            const searchResults: StructuredPropertyDefinition[] = [
                { urn: 'urn:search1', definition: { displayName: 'Search 1' } },
            ];
            const selectedPropertyData: StructuredPropertyDefinition = {
                urn: 'urn:selected',
                definition: { displayName: 'Selected' },
            };
            const result = buildPropertyOptionsMap(searchResults, selectedPropertyData, 'urn:unfetched');

            expect(result.size).toBe(3);
            expect(result.has('urn:search1')).toBe(true);
            expect(result.has('urn:selected')).toBe(true);
            expect(result.has('urn:unfetched')).toBe(true);
        });

        it('avoids duplicate entries', () => {
            const searchResults: StructuredPropertyDefinition[] = [
                { urn: 'urn:prop1', definition: { displayName: 'Property 1' } },
            ];
            const selectedPropertyData: StructuredPropertyDefinition = {
                urn: 'urn:prop1',
                definition: { displayName: 'Updated Property 1' },
            };
            const result = buildPropertyOptionsMap(searchResults, selectedPropertyData, 'urn:prop1');

            expect(result.size).toBe(1);
            expect(result.get('urn:prop1')?.label).toBe('Updated Property 1');
        });
    });

    describe('buildDefinitionsMap', () => {
        it('builds empty map for empty options', () => {
            const result = buildDefinitionsMap([]);
            expect(result.size).toBe(0);
        });

        it('builds definitions map from property options', () => {
            const options = [
                { value: 'urn:prop1', label: 'Property 1' },
                { value: 'urn:prop2', label: 'Property 2' },
            ];
            const result = buildDefinitionsMap(options);

            expect(result.size).toBe(2);
            expect(result.get('urn:prop1')).toEqual({
                urn: 'urn:prop1',
                definition: { displayName: 'Property 1' },
            });
        });

        it('handles options with URN as label (no display name)', () => {
            const options = [{ value: 'urn:prop1', label: 'urn:prop1' }];
            const result = buildDefinitionsMap(options);

            expect(result.get('urn:prop1')?.definition?.displayName).toBeUndefined();
        });
    });
});
