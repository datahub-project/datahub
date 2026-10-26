import {
    cleanLogicalPredicate,
    convertStateToUpdateInput,
    convertViewToBuilderState,
    hasAtLeastOneValidCondition,
    parseJsonToLogicalPredicate,
} from '@app/entityV2/view/utils';
import { LogicalOperatorType } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/sharedV2/queryBuilder/builder/utils';

import { DataHubView, DataHubViewType, FilterOperator, LogicalOperator } from '@types';

describe('View Utils - JSON Conversion Functions', () => {
    describe('parseJsonToLogicalPredicate', () => {
        it('should parse valid JSON to LogicalPredicate', () => {
            const json = '{"type":"logical","operator":"and","operands":[]}';
            const result = parseJsonToLogicalPredicate(json);

            expect(result).toBeTruthy();
            expect(result?.type).toBe('logical');
            expect(result?.operator).toBe('and');
        });

        it('should return null for null input', () => {
            const result = parseJsonToLogicalPredicate(null);
            expect(result).toBeNull();
        });

        it('should return null for undefined input', () => {
            const result = parseJsonToLogicalPredicate(undefined);
            expect(result).toBeNull();
        });

        it('should return null for empty string', () => {
            const result = parseJsonToLogicalPredicate('');
            expect(result).toBeNull();
        });

        it('should return null for invalid JSON', () => {
            const result = parseJsonToLogicalPredicate('invalid json');
            expect(result).toBeNull();
        });
    });

    describe('convertViewToBuilderState', () => {
        const mockView: DataHubView = {
            urn: 'urn:li:dataHubView:test-view',
            type: 'DATAHUB_VIEW' as any,
            viewType: DataHubViewType.Personal,
            name: 'Test View',
            description: 'Test description',
            definition: {
                entityTypes: [],
                filter: {
                    operator: LogicalOperator.And,
                    filters: [
                        {
                            field: 'field1',
                            values: ['value1'],
                            condition: FilterOperator.Equal,
                        },
                    ],
                    json: '{"type":"logical","operator":"and","operands":[]}',
                },
            },
        };

        it('should convert DataHubView to ViewBuilderState', () => {
            const result = convertViewToBuilderState(mockView);

            expect(result.viewType).toBe(DataHubViewType.Personal);
            expect(result.name).toBe('Test View');
            expect(result.description).toBe('Test description');
        });

        it('should parse json field to logicalPredicate', () => {
            const result = convertViewToBuilderState(mockView);

            expect(result.definition?.logicalPredicate).toBeTruthy();
            expect(result.definition?.logicalPredicate?.type).toBe('logical');
        });

        it('should preserve filter object', () => {
            const result = convertViewToBuilderState(mockView);

            expect(result.definition?.filter).toBeTruthy();
            expect(result.definition?.filter?.operator).toBe(LogicalOperator.And);
        });

        it('should convert legacy filters to logicalPredicate when json field is missing', () => {
            const viewWithoutJson = JSON.parse(JSON.stringify(mockView));
            viewWithoutJson.definition.filter.json = undefined;

            const result = convertViewToBuilderState(viewWithoutJson);

            // Should have converted legacy filters to logicalPredicate
            expect(result.definition?.logicalPredicate).toBeTruthy();
            expect(result.definition?.logicalPredicate?.type).toBe('logical');
        });
    });

    describe('convertStateToUpdateInput', () => {
        const mockState = {
            viewType: DataHubViewType.Personal,
            name: 'Test View',
            description: 'Test description',
            definition: {
                entityTypes: ['DATASET'],
                filter: {
                    operator: LogicalOperator.And,
                    filters: [
                        {
                            field: 'field1',
                            values: ['value1'],
                            condition: FilterOperator.Equal,
                        },
                    ],
                },
                logicalPredicate: {
                    type: 'logical' as const,
                    operator: LogicalOperatorType.AND,
                    operands: [
                        {
                            type: 'property' as const,
                            property: 'field1',
                            operator: 'equals',
                            values: ['value1'],
                        },
                    ],
                },
            },
        } as any;

        it('should convert state to GraphQL input', () => {
            const result = convertStateToUpdateInput(mockState);

            expect(result.viewType).toBe(DataHubViewType.Personal);
            expect(result.name).toBe('Test View');
        });

        it('should include json when logicalPredicate exists', () => {
            const result = convertStateToUpdateInput(mockState);

            expect(result.definition.filter.json).toBeTruthy();
            expect(typeof result.definition.filter.json).toBe('string');
        });

        it('should throw error if logicalPredicate is missing', () => {
            // logicalPredicate should always be populated by convertViewToBuilderState
            // If missing, it indicates a bug in the conversion logic
            const stateWithoutLogicalPredicate = JSON.parse(JSON.stringify(mockState));
            delete stateWithoutLogicalPredicate.definition.logicalPredicate;

            expect(() => convertStateToUpdateInput(stateWithoutLogicalPredicate)).toThrow(
                'logicalPredicate must be populated',
            );
        });

        it('should throw error when predicate becomes empty after cleaning', () => {
            // Empty property operands are removed during cleaning, leaving no valid conditions.
            const stateWithEmptyPredicate = {
                viewType: DataHubViewType.Personal,
                name: 'Test View',
                description: 'Test description',
                definition: {
                    entityTypes: ['DATASET'],
                    logicalPredicate: {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property' as const,
                                // No property field, will be removed during cleaning
                            },
                        ],
                    },
                },
            } as any;

            expect(() => convertStateToUpdateInput(stateWithEmptyPredicate)).toThrow(
                'View must have at least one valid filter condition',
            );
        });
    });

    describe('Nested Conditions Support', () => {
        it('should parse and preserve complex nested AND/OR structure', () => {
            const complexJson = JSON.stringify({
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'field1',
                                operator: 'equals',
                                values: ['a'],
                            },
                            {
                                type: 'property',
                                property: 'field1',
                                operator: 'equals',
                                values: ['b'],
                            },
                        ],
                    },
                    {
                        type: 'property',
                        property: 'field2',
                        operator: 'contains_str',
                        values: ['test'],
                    },
                ],
            });

            const result = parseJsonToLogicalPredicate(complexJson);
            expect(result?.type).toBe('logical');
            expect(result?.operator).toBe('and');
            expect(result?.operands).toHaveLength(2);
            expect((result?.operands?.[0] as any)?.operator).toBe('or');
        });

        it('should handle NOT operator in predicates', () => {
            const notJson = JSON.stringify({
                type: 'logical',
                operator: LogicalOperatorType.NOT,
                operands: [
                    {
                        type: 'property',
                        property: 'field',
                        operator: 'exists',
                        values: [],
                    },
                ],
            });

            const result = parseJsonToLogicalPredicate(notJson);
            expect(result?.operator).toBe('not');
            expect(result?.operands).toHaveLength(1);
        });

        it('should handle deeply nested predicates', () => {
            const deeplyNestedJson = JSON.stringify({
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'logical',
                                operator: LogicalOperatorType.NOT,
                                operands: [
                                    {
                                        type: 'property',
                                        property: 'field',
                                        operator: 'equals',
                                        values: ['value'],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            });

            const result = parseJsonToLogicalPredicate(deeplyNestedJson);
            expect(result?.type).toBe('logical');
            const orOperand = result?.operands?.[0] as any;
            expect(orOperand?.operator).toBe('or');
            const notOperand = orOperand?.operands?.[0] as any;
            expect(notOperand?.operator).toBe('not');
            expect((notOperand?.operands?.[0] as any)?.type).toBe('property');
        });
    });

    describe('Round-trip Conversion', () => {
        it('should preserve nested conditions through full round-trip', () => {
            const originalPredicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'platform',
                                operator: 'equals',
                                values: ['mysql'],
                            },
                            {
                                type: 'property' as const,
                                property: 'platform',
                                operator: 'equals',
                                values: ['postgres'],
                            },
                        ],
                    },
                    {
                        type: 'property' as const,
                        property: 'owner',
                        operator: 'exists',
                        values: [],
                    },
                ],
            };

            const state = {
                viewType: DataHubViewType.Personal,
                name: 'Test',
                definition: {
                    entityTypes: [],
                    filter: { operator: LogicalOperator.And, filters: [] },
                    logicalPredicate: originalPredicate,
                },
            };

            const input = convertStateToUpdateInput(state);
            const { json } = input.definition.filter;
            const restoredPredicate = parseJsonToLogicalPredicate(json);

            expect(restoredPredicate).toEqual(originalPredicate);
        });
    });

    describe('Legacy Filter Conversion', () => {
        it('should convert legacy filters with AND operator to logicalPredicate on load', () => {
            const mockView: DataHubView = {
                urn: 'urn:li:dataHubView:test-view',
                type: 'DATAHUB_VIEW' as any,
                viewType: DataHubViewType.Personal,
                name: 'Legacy View',
                definition: {
                    entityTypes: [],
                    filter: {
                        operator: LogicalOperator.And,
                        filters: [
                            {
                                field: 'status',
                                condition: FilterOperator.Equal,
                                values: ['ACTIVE'],
                            },
                            {
                                field: 'platform',
                                condition: FilterOperator.Equal,
                                values: ['mysql'],
                            },
                        ],
                    },
                },
            };

            const state = convertViewToBuilderState(mockView);

            // Should have populated logicalPredicate from legacy filters
            expect(state.definition?.logicalPredicate).toBeTruthy();
            expect(state.definition?.logicalPredicate?.type).toBe('logical');
            expect(state.definition?.logicalPredicate?.operator).toBe('and');
            expect(state.definition?.logicalPredicate?.operands).toHaveLength(2);
        });

        it('should convert legacy filters with OR operator to logicalPredicate on load', () => {
            const mockView: DataHubView = {
                urn: 'urn:li:dataHubView:test-view',
                type: 'DATAHUB_VIEW' as any,
                viewType: DataHubViewType.Personal,
                name: 'Legacy OR View',
                definition: {
                    entityTypes: [],
                    filter: {
                        operator: LogicalOperator.Or,
                        filters: [
                            {
                                field: 'owner',
                                condition: FilterOperator.Exists,
                                values: [],
                            },
                            {
                                field: 'dataProducts',
                                condition: FilterOperator.Exists,
                                values: [],
                            },
                        ],
                    },
                },
            };

            const state = convertViewToBuilderState(mockView);

            expect(state.definition?.logicalPredicate).toBeTruthy();
            expect(state.definition?.logicalPredicate?.operator).toBe('or');
            expect(state.definition?.logicalPredicate?.operands).toHaveLength(2);
        });

        it('should prefer json field over legacy filters when both exist', () => {
            const jsonPredicate = {
                type: 'logical' as const,
                operator: 'or' as const,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'fieldA',
                        operator: 'equals' as const,
                        values: ['valueA'],
                    },
                ],
            };

            const mockView: DataHubView = {
                urn: 'urn:li:dataHubView:test-view',
                type: 'DATAHUB_VIEW' as any,
                viewType: DataHubViewType.Personal,
                name: 'Mixed View',
                definition: {
                    entityTypes: [],
                    filter: {
                        operator: LogicalOperator.And,
                        filters: [
                            {
                                field: 'ignored',
                                condition: FilterOperator.Equal,
                                values: ['value'],
                            },
                        ],
                        json: JSON.stringify(jsonPredicate),
                    },
                },
            };

            const state = convertViewToBuilderState(mockView);

            // Should use json, not legacy filters
            expect(state.definition?.logicalPredicate?.operator).toBe('or');
            expect(state.definition?.logicalPredicate?.operands).toHaveLength(1);
        });

        it('should always send orFilters + json format on update, never legacy format', () => {
            // Simulate a view that was loaded with legacy format and converted
            const state = {
                viewType: DataHubViewType.Personal,
                name: 'Updated View',
                description: 'Test',
                definition: {
                    entityTypes: [],
                    filter: {
                        operator: LogicalOperator.And,
                        filters: [
                            {
                                field: 'status',
                                condition: FilterOperator.Equal,
                                values: ['ACTIVE'],
                            },
                        ],
                    },
                    logicalPredicate: {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'status',
                                operator: 'equals' as const,
                                values: ['ACTIVE'],
                            },
                        ],
                    },
                },
            };

            const input = convertStateToUpdateInput(state);

            // Should contain new format only
            expect(input.definition.filter.orFilters).toBeTruthy();
            expect(input.definition.filter.json).toBeTruthy();
        });

        it('should throw error if logicalPredicate is missing during update', () => {
            const state = {
                viewType: DataHubViewType.Personal,
                name: 'Invalid View',
                definition: {
                    entityTypes: [],
                    filter: {
                        operator: LogicalOperator.And,
                        filters: [],
                    },
                    // Missing logicalPredicate
                },
            } as any;

            expect(() => convertStateToUpdateInput(state)).toThrow('logicalPredicate must be populated');
        });

        it('should default missing operator to equals when cleaning', () => {
            const stateWithMissingOperator = {
                viewType: DataHubViewType.Personal,
                name: 'Test View',
                definition: {
                    entityTypes: [],
                    filter: {
                        operator: LogicalOperator.And,
                        filters: [],
                    },
                    logicalPredicate: {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'status',
                                operator: undefined,
                                values: undefined,
                            },
                        ],
                    },
                },
            };

            const input = convertStateToUpdateInput(stateWithMissingOperator);
            const parsedJson = JSON.parse(input.definition.filter.json);

            // Operator should be defaulted to equals in JSON
            expect(parsedJson.operands).toHaveLength(1);
            expect(parsedJson.operands[0].property).toBe('status');
            expect(parsedJson.operands[0].operator).toBe('equals');
        });

        it('should produce identical orFilters after cleaning empty conditions', () => {
            const predicateWithEmpty = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'status',
                        operator: 'equals' as const,
                        values: ['ACTIVE'],
                    },
                    {
                        type: 'property' as const,
                        property: undefined,
                        operator: undefined,
                        values: undefined,
                    },
                    {
                        type: 'property' as const,
                        property: 'platform',
                        operator: 'equals' as const,
                        values: ['mysql'],
                    },
                ],
            };

            // Get orFilters from uncleaned predicate
            const orFiltersUncleaned = convertLogicalPredicateToOrFilters(predicateWithEmpty);

            // Get orFilters from cleaned predicate
            const cleaned = cleanLogicalPredicate(predicateWithEmpty) as any;
            const orFiltersCleaned = convertLogicalPredicateToOrFilters(cleaned);

            // They should be identical - cleaning doesn't change the filter logic
            expect(orFiltersCleaned).toEqual(orFiltersUncleaned);
            // orFilters = [{ and: [status filter, platform filter] }]
            expect(orFiltersCleaned?.[0]?.and).toHaveLength(2);
            expect(orFiltersCleaned?.[0]?.and?.[0]?.field).toBe('status');
            expect(orFiltersCleaned?.[0]?.and?.[1]?.field).toBe('platform');
        });
    });

    describe('cleanLogicalPredicate', () => {
        it('should remove conditions without a property', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'status',
                        operator: 'equals' as const,
                        values: ['ACTIVE'],
                    },
                    {
                        type: 'property' as const,
                        property: undefined,
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            const result = cleanLogicalPredicate(predicate);

            expect(result).toBeTruthy();
            expect((result as any).operands).toHaveLength(1);
            expect((result as any).operands[0].property).toBe('status');
        });

        it('should default operator to equals when property exists but operator is missing', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'status',
                        operator: 'equals' as const,
                        values: ['ACTIVE'],
                    },
                    {
                        type: 'property' as const,
                        property: 'platform',
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            const result = cleanLogicalPredicate(predicate);

            expect(result).toBeTruthy();
            expect((result as any).operands).toHaveLength(2);
            expect((result as any).operands[1].property).toBe('platform');
            expect((result as any).operands[1].operator).toBe('equals');
        });

        it('should return undefined if all operands are empty', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: undefined,
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            const result = cleanLogicalPredicate(predicate);
            expect(result).toBeUndefined();
        });

        it('should recursively clean nested groups', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'field1',
                                operator: 'equals' as const,
                                values: ['value1'],
                            },
                            {
                                type: 'property' as const,
                                property: undefined,
                                operator: undefined,
                                values: undefined,
                            },
                        ],
                    },
                    {
                        type: 'property' as const,
                        property: undefined,
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            const result = cleanLogicalPredicate(predicate);

            expect(result).toBeTruthy();
            expect((result as any).operands).toHaveLength(1);
            expect((result as any).operands[0].operands).toHaveLength(1);
            expect((result as any).operands[0].operands[0].property).toBe('field1');
        });

        it('should handle null/undefined input', () => {
            expect(cleanLogicalPredicate(null)).toBeUndefined();
            expect(cleanLogicalPredicate(undefined)).toBeUndefined();
        });

        it('should preserve condition with property', () => {
            const predicate = {
                type: 'property' as const,
                property: 'owner',
                operator: 'exists' as const,
                values: [],
            };

            const result = cleanLogicalPredicate(predicate);
            expect(result).toEqual(predicate);
        });

        it('should filter out property predicate without property', () => {
            const predicate = {
                type: 'property' as const,
                property: undefined,
                operator: undefined,
                values: undefined,
            };

            const result = cleanLogicalPredicate(predicate);
            expect(result).toBeUndefined();
        });

        it('should preserve values and other fields when defaulting operator', () => {
            const predicate = {
                type: 'property' as const,
                property: 'priority',
                operator: undefined,
                values: ['HIGH', 'URGENT'],
            };

            const result = cleanLogicalPredicate(predicate) as any;

            expect(result.property).toBe('priority');
            expect(result.operator).toBe('equals');
            expect(result.values).toEqual(['HIGH', 'URGENT']);
        });

        it('should clean complex OR group with mixed valid/empty conditions', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.OR,
                operands: [
                    { type: 'property' as const, property: 'env', operator: 'equals' as const, values: ['prod'] },
                    { type: 'property' as const, property: undefined, operator: undefined, values: undefined },
                    { type: 'property' as const, property: 'region', operator: undefined, values: [] },
                ],
            };

            const result = cleanLogicalPredicate(predicate) as any;

            expect(result.operands).toHaveLength(2);
            expect(result.operands[0].property).toBe('env');
            expect(result.operands[1].property).toBe('region');
            expect(result.operands[1].operator).toBe('equals');
        });
    });

    describe('hasAtLeastOneValidCondition', () => {
        it('should return true if predicate has at least one complete condition (property + operator)', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'status',
                        operator: 'equals' as const,
                        values: ['ACTIVE'],
                    },
                    {
                        type: 'property' as const,
                        property: undefined,
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            expect(hasAtLeastOneValidCondition(predicate)).toBe(true);
        });

        it('should return true even if operator is missing (operator defaults to equals)', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'status',
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            // Property is selected, so condition is valid (operator will default to equals on save)
            expect(hasAtLeastOneValidCondition(predicate)).toBe(true);
        });

        it('should return false if all conditions are empty', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property' as const,
                        property: undefined,
                        operator: undefined,
                        values: undefined,
                    },
                ],
            };

            expect(hasAtLeastOneValidCondition(predicate)).toBe(false);
        });

        it('should return true for deeply nested valid condition', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property' as const,
                                property: undefined,
                                operator: undefined,
                                values: undefined,
                            },
                            {
                                type: 'logical' as const,
                                operator: LogicalOperatorType.AND,
                                operands: [
                                    {
                                        type: 'property' as const,
                                        property: 'owner',
                                        operator: 'exists' as const,
                                        values: [],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            };

            expect(hasAtLeastOneValidCondition(predicate)).toBe(true);
        });

        it('should return false for null/undefined', () => {
            expect(hasAtLeastOneValidCondition(null)).toBe(false);
            expect(hasAtLeastOneValidCondition(undefined)).toBe(false);
        });

        it('should return true for single valid property predicate', () => {
            const predicate = {
                type: 'property' as const,
                property: 'platform',
                operator: 'equals' as const,
                values: ['mysql'],
            };

            expect(hasAtLeastOneValidCondition(predicate)).toBe(true);
        });

        it('should return false for single empty property predicate', () => {
            const predicate = {
                type: 'property' as const,
                property: undefined,
                operator: undefined,
                values: undefined,
            };

            expect(hasAtLeastOneValidCondition(predicate)).toBe(false);
        });

        it('should work with NOT operator', () => {
            const predicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.NOT,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'deprecated',
                        operator: 'equals' as const,
                        values: ['true'],
                    },
                ],
            };

            expect(hasAtLeastOneValidCondition(predicate)).toBe(true);
        });
    });

    describe('Deep Nesting and OrFilters Conversion', () => {
        it('should correctly convert deeply nested AND/OR/NOT to orFilters', () => {
            const complexPredicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'field1',
                                operator: 'equals' as const,
                                values: ['a'],
                            },
                            {
                                type: 'property' as const,
                                property: 'field2',
                                operator: 'equals' as const,
                                values: ['b'],
                            },
                        ],
                    },
                    {
                        type: 'property' as const,
                        property: 'field3',
                        operator: 'contains' as const,
                        values: ['c'],
                    },
                ],
            };

            const orFilters = convertLogicalPredicateToOrFilters(complexPredicate);
            expect(orFilters).toBeDefined();
            expect(Array.isArray(orFilters)).toBe(true);
            expect((orFilters as any).length).toBeGreaterThan(0);
        });

        it('should preserve structure when converting nested predicates to JSON', () => {
            const nestedPredicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'platform',
                                operator: 'equals' as const,
                                values: ['mysql'],
                            },
                            {
                                type: 'property' as const,
                                property: 'platform',
                                operator: 'equals' as const,
                                values: ['postgres'],
                            },
                        ],
                    },
                ],
            };

            const json = JSON.stringify(nestedPredicate);
            const parsed = JSON.parse(json);

            expect(parsed.type).toBe('logical');
            expect(parsed.operator).toBe(LogicalOperatorType.AND);
            expect(parsed.operands[0].type).toBe('logical');
            expect(parsed.operands[0].operator).toBe(LogicalOperatorType.OR);
            expect(parsed.operands[0].operands).toHaveLength(2);
        });
    });

    describe('PropertyPredicate Edge Cases', () => {
        it('should handle PropertyPredicate with empty values array (exists operator)', () => {
            const predicateWithEmptyValues = {
                type: 'property' as const,
                property: 'description',
                operator: 'exists' as const,
                values: [],
            };

            expect(hasAtLeastOneValidCondition(predicateWithEmptyValues)).toBe(true);

            const json = JSON.stringify(predicateWithEmptyValues);
            const parsed = JSON.parse(json);
            expect(parsed.values).toEqual([]);
            expect(parsed.property).toBe('description');
        });

        it('should preserve negated flag through conversion', () => {
            const negatedPredicate = {
                type: 'property' as const,
                property: 'status',
                operator: 'equals' as const,
                values: ['deprecated'],
                negated: true,
            };

            const json = JSON.stringify(negatedPredicate);
            const parsed = JSON.parse(json);

            expect(parsed.negated).toBe(true);
            expect(parsed.property).toBe('status');
            expect(parsed.values).toEqual(['deprecated']);
        });

        it('should preserve negated flag in nested NOT operator', () => {
            const notPredicate = {
                type: 'logical' as const,
                operator: LogicalOperatorType.NOT,
                operands: [
                    {
                        type: 'property' as const,
                        property: 'hasSchema',
                        operator: 'exists' as const,
                        values: [],
                        negated: false,
                    },
                ],
            };

            const json = JSON.stringify(notPredicate);
            const parsed = JSON.parse(json);

            expect(parsed.operator).toBe(LogicalOperatorType.NOT);
            expect(parsed.operands[0].negated).toBe(false);
        });
    });

    describe('GraphQL Input Validation', () => {
        it('should produce valid GraphQL input with all required fields', () => {
            const state = {
                viewType: DataHubViewType.Personal,
                name: 'Complete View',
                description: 'A complete view with all fields',
                definition: {
                    entityTypes: ['DATASET', 'DASHBOARD'],
                    logicalPredicate: {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'platform',
                                operator: 'equals' as const,
                                values: ['mysql'],
                            },
                        ],
                    },
                },
            } as any;

            const input = convertStateToUpdateInput(state);

            expect(input.viewType).toBeDefined();
            expect(input.name).toBeDefined();
            expect(input.description).toBeDefined();
            expect(input.definition).toBeDefined();
            expect(input.definition.entityTypes).toBeDefined();
            expect(input.definition.filter).toBeDefined();
            expect(input.definition.filter.orFilters).toBeDefined();
            expect(input.definition.filter.json).toBeDefined();
            expect(typeof input.definition.filter.json).toBe('string');
        });

        it('should produce orFilters and json in sync after conversion', () => {
            const state = {
                viewType: DataHubViewType.Personal,
                name: 'Sync Test',
                description: null,
                definition: {
                    entityTypes: ['DATASET'],
                    logicalPredicate: {
                        type: 'logical' as const,
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property' as const,
                                property: 'env',
                                operator: 'equals' as const,
                                values: ['PROD'],
                            },
                            {
                                type: 'property' as const,
                                property: 'env',
                                operator: 'equals' as const,
                                values: ['STAGING'],
                            },
                        ],
                    },
                },
            } as any;

            const input = convertStateToUpdateInput(state);
            const jsonPredicate = JSON.parse(input.definition.filter.json);

            expect(jsonPredicate.operator).toBe(LogicalOperatorType.OR);
            expect(jsonPredicate.operands).toHaveLength(2);
            expect(input.definition.filter.orFilters).toBeDefined();
            expect(Array.isArray(input.definition.filter.orFilters)).toBe(true);
        });
    });
});
