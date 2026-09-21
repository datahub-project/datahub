import {
    convertStateToUpdateInput,
    convertViewToBuilderState,
    parseJsonToLogicalPredicate,
} from '@app/entityV2/view/utils';

import { DataHubView, DataHubViewType, FilterOperator, LogicalOperator } from '@types';

describe('View Utils - JSON Conversion Functions', () => {
    describe('parseJsonToLogicalPredicate', () => {
        it('should parse valid JSON to LogicalPredicate', () => {
            const json = '{"type":"logical","operator":"AND","operands":[]}';
            const result = parseJsonToLogicalPredicate(json);

            expect(result).toBeTruthy();
            expect(result?.type).toBe('logical');
            expect(result?.operator).toBe('AND');
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
                    json: '{"type":"logical","operator":"AND","operands":[]}',
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

        it('should handle view without json field', () => {
            const viewWithoutJson = JSON.parse(JSON.stringify(mockView));
            viewWithoutJson.definition.filter.json = undefined;

            const result = convertViewToBuilderState(viewWithoutJson);

            expect(result.definition?.logicalPredicate).toBeUndefined();
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
                    operator: 'AND' as any,
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

        it('should preserve old format fields', () => {
            // When logicalPredicate exists, we send NEW format only (orFilters + json), not legacy format
            const stateWithoutLogicalPredicate = JSON.parse(JSON.stringify(mockState));
            delete stateWithoutLogicalPredicate.definition.logicalPredicate;

            const result = convertStateToUpdateInput(stateWithoutLogicalPredicate);

            expect(result.definition.filter.operator).toBe(LogicalOperator.And);
            expect(result.definition.filter.filters).toBeTruthy();
        });

        it('should handle state without logicalPredicate', () => {
            const stateWithoutLogicalPredicate = JSON.parse(JSON.stringify(mockState));
            delete stateWithoutLogicalPredicate.definition.logicalPredicate;

            const result = convertStateToUpdateInput(stateWithoutLogicalPredicate);

            expect(result.definition.filter.json).toBeUndefined();
        });
    });

    describe('Nested Conditions Support', () => {
        it('should parse and preserve complex nested AND/OR structure', () => {
            const complexJson = JSON.stringify({
                type: 'logical',
                operator: 'AND',
                operands: [
                    {
                        type: 'logical',
                        operator: 'OR',
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
            expect(result?.operator).toBe('AND');
            expect(result?.operands).toHaveLength(2);
            expect((result?.operands?.[0] as any)?.operator).toBe('OR');
        });

        it('should handle NOT operator in predicates', () => {
            const notJson = JSON.stringify({
                type: 'logical',
                operator: 'NOT',
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
            expect(result?.operator).toBe('NOT');
            expect(result?.operands).toHaveLength(1);
        });

        it('should handle deeply nested predicates', () => {
            const deeplyNestedJson = JSON.stringify({
                type: 'logical',
                operator: 'AND',
                operands: [
                    {
                        type: 'logical',
                        operator: 'OR',
                        operands: [
                            {
                                type: 'logical',
                                operator: 'NOT',
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
            expect(orOperand?.operator).toBe('OR');
            const notOperand = orOperand?.operands?.[0] as any;
            expect(notOperand?.operator).toBe('NOT');
            expect((notOperand?.operands?.[0] as any)?.type).toBe('property');
        });
    });

    describe('Round-trip Conversion', () => {
        it('should preserve nested conditions through full round-trip', () => {
            const originalPredicate = {
                type: 'logical' as const,
                operator: 'AND' as any,
                operands: [
                    {
                        type: 'logical' as const,
                        operator: 'OR' as any,
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
});
