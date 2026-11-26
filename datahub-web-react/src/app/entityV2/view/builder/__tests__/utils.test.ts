import { BUILD_FILTERS_TAB_KEY, SELECT_ASSETS_TAB_KEY, URN_FILTER_NAME } from '@app/entityV2/view/builder/constants';
import {
    convertNestedSubTypeFilter,
    extractUrnsFromLogicalPredicate,
    filtersToLogicalPredicate,
    filtersToSelectedUrns,
    getInitialTabKey,
    resolveFilterValues,
} from '@app/entityV2/view/builder/utils';
import { ENTITY_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '@app/search/utils/constants';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { FilterOperator, LogicalOperator } from '@src/types.generated';

describe('view builder utils', () => {
    it('should convert the nested subtypes filter properly along with other filters', () => {
        const filters = [
            { field: 'platform', values: ['platform1', 'platform2'] },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASETS', 'CONTAINERS␞schema'] },
            { field: 'tag', values: ['tag1', 'tag2'] },
        ];

        expect(convertNestedSubTypeFilter(filters)).toMatchObject([
            { field: 'platform', values: ['platform1', 'platform2'] },
            { field: 'tag', values: ['tag1', 'tag2'] },
            { field: ENTITY_FILTER_NAME, values: ['DATASETS'] },
            { field: TYPE_NAMES_FILTER_NAME, values: ['schema'] },
        ]);
    });

    describe('resolveFilterValues', () => {
        it('should return ["true"] for is_true operator', () => {
            const prop: PropertyPredicate = {
                type: 'property',
                property: 'hasSchema',
                operator: 'is_true',
                values: [],
            };
            expect(resolveFilterValues(prop)).toEqual(['true']);
        });

        it('should return ["false"] for is_false operator', () => {
            const prop: PropertyPredicate = {
                type: 'property',
                property: 'hasOwner',
                operator: 'is_false',
                values: [],
            };
            expect(resolveFilterValues(prop)).toEqual(['false']);
        });

        it('should return original values for non-boolean operators', () => {
            const prop: PropertyPredicate = {
                type: 'property',
                property: 'platform',
                operator: 'equals',
                values: ['mysql', 'postgres'],
            };
            expect(resolveFilterValues(prop)).toEqual(['mysql', 'postgres']);
        });

        it('should return empty array if no values provided for non-boolean operator', () => {
            const prop: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'exists',
            };
            expect(resolveFilterValues(prop)).toEqual([]);
        });
    });

    describe('extractUrnsFromLogicalPredicate', () => {
        it('should extract URN from simple PropertyPredicate', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: URN_FILTER_NAME,
                operator: 'equals',
                values: ['urn:li:dataset:test1', 'urn:li:dataset:test2'],
            };
            expect(extractUrnsFromLogicalPredicate(predicate)).toEqual([
                'urn:li:dataset:test1',
                'urn:li:dataset:test2',
            ]);
        });

        it('should return empty array for PropertyPredicate without URN field', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'platform',
                operator: 'equals',
                values: ['mysql'],
            };
            expect(extractUrnsFromLogicalPredicate(predicate)).toEqual([]);
        });

        it('should find URN in nested LogicalPredicate with AND operator', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'platform',
                        operator: 'equals',
                        values: ['mysql'],
                    },
                    {
                        type: 'property',
                        property: URN_FILTER_NAME,
                        operator: 'equals',
                        values: ['urn:li:dataset:selected1'],
                    },
                ],
            };
            expect(extractUrnsFromLogicalPredicate(predicate)).toEqual(['urn:li:dataset:selected1']);
        });

        it('should find URN in nested LogicalPredicate with OR operator', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.OR,
                operands: [
                    {
                        type: 'property',
                        property: 'tag',
                        operator: 'equals',
                        values: ['important'],
                    },
                    {
                        type: 'property',
                        property: URN_FILTER_NAME,
                        operator: 'equals',
                        values: ['urn:li:dataset:selected2'],
                    },
                ],
            };
            expect(extractUrnsFromLogicalPredicate(predicate)).toEqual(['urn:li:dataset:selected2']);
        });

        it('should handle deeply nested LogicalPredicates', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'platform',
                                operator: 'equals',
                                values: ['mysql'],
                            },
                            {
                                type: 'property',
                                property: 'tag',
                                operator: 'equals',
                                values: ['prod'],
                            },
                        ],
                    },
                    {
                        type: 'property',
                        property: URN_FILTER_NAME,
                        operator: 'equals',
                        values: ['urn:li:dataset:deep'],
                    },
                ],
            };
            expect(extractUrnsFromLogicalPredicate(predicate)).toEqual(['urn:li:dataset:deep']);
        });

        it('should return empty array for null/undefined predicate', () => {
            expect(extractUrnsFromLogicalPredicate(null)).toEqual([]);
            expect(extractUrnsFromLogicalPredicate(undefined)).toEqual([]);
        });

        it('should return empty array for LogicalPredicate with no URN', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'platform',
                        operator: 'equals',
                        values: ['mysql'],
                    },
                ],
            };
            expect(extractUrnsFromLogicalPredicate(predicate)).toEqual([]);
        });
    });

    describe('filtersToLogicalPredicate', () => {
        it('should convert simple filters to AND LogicalPredicate', () => {
            const filters = [
                { field: 'platform', values: ['mysql'], condition: FilterOperator.Equal },
                { field: 'type', values: ['DATASET'], condition: FilterOperator.Equal },
            ];
            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(2);
        });

        it('should convert filters to OR LogicalPredicate', () => {
            const filters = [
                { field: 'tag', values: ['tag1'], condition: FilterOperator.Equal },
                { field: 'tag', values: ['tag2'], condition: FilterOperator.Equal },
            ];
            const result = filtersToLogicalPredicate(LogicalOperator.Or, filters);

            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.OR);
            expect(result.operands).toHaveLength(2);
        });

        it('should handle boolean operators by clearing values and setting correct operator', () => {
            const filters = [{ field: 'hasSchema', values: [], condition: FilterOperator.Equal }];
            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            const operand = result.operands[0] as PropertyPredicate;
            expect(operand.operator).toBe('equals');
            expect(operand.values).toEqual([]);
        });

        it('should wrap all negated filters in NOT operator', () => {
            const filters = [
                { field: 'platform', values: ['mysql'], condition: FilterOperator.Equal, negated: true },
                { field: 'type', values: ['DATASET'], condition: FilterOperator.Equal, negated: true },
            ];
            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            expect(result.operator).toBe(LogicalOperatorType.NOT);
            expect(result.operands).toHaveLength(2);
        });

        it('should preserve non-negated filters in standard AND/OR', () => {
            const filters = [
                { field: 'platform', values: ['mysql'], condition: FilterOperator.Equal, negated: false },
                { field: 'type', values: ['DATASET'], condition: FilterOperator.Equal },
            ];
            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(2);
        });

        it('should handle mixed negation (some negated, some not)', () => {
            const filters = [
                { field: 'platform', values: ['mysql'], condition: FilterOperator.Equal },
                { field: 'type', values: ['DATASET'], condition: FilterOperator.Equal, negated: true },
            ];
            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            expect(result.operator).toBe(LogicalOperatorType.AND);
            // Negated operand is wrapped in NOT operator
            const secondOperand = result.operands[1] as LogicalPredicate;
            expect(secondOperand.type).toBe('logical');
            expect(secondOperand.operator).toBe(LogicalOperatorType.NOT);
            expect(secondOperand.operands[0].type).toBe('property');
        });

        it('should handle empty filters array', () => {
            const result = filtersToLogicalPredicate(LogicalOperator.And, []);

            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(0);
        });
    });

    describe('filtersToSelectedUrns', () => {
        it('should extract URNs from filters array with urn field', () => {
            const filters = [{ field: URN_FILTER_NAME, values: ['urn:li:dataset:test1', 'urn:li:dataset:test2'] }];
            expect(filtersToSelectedUrns(filters)).toEqual(['urn:li:dataset:test1', 'urn:li:dataset:test2']);
        });

        it('should return empty array if no urn filter in filters', () => {
            const filters = [
                { field: 'platform', values: ['mysql'] },
                { field: 'type', values: ['DATASET'] },
            ];
            expect(filtersToSelectedUrns(filters)).toEqual([]);
        });

        it('should fall back to JSON when filters array is empty', () => {
            const json = JSON.stringify({
                type: 'logical',
                operator: 'or',
                operands: [
                    {
                        type: 'property',
                        property: URN_FILTER_NAME,
                        operator: 'equals',
                        values: ['urn:li:dataset:from_json'],
                    },
                ],
            });
            expect(filtersToSelectedUrns([], json)).toEqual(['urn:li:dataset:from_json']);
        });

        it('should prefer filters array over JSON', () => {
            const filters = [{ field: URN_FILTER_NAME, values: ['urn:li:dataset:from_filters'] }];
            const json = JSON.stringify({
                type: 'logical',
                operator: 'or',
                operands: [
                    {
                        type: 'property',
                        property: URN_FILTER_NAME,
                        operator: 'equals',
                        values: ['urn:li:dataset:from_json'],
                    },
                ],
            });
            expect(filtersToSelectedUrns(filters, json)).toEqual(['urn:li:dataset:from_filters']);
        });

        it('should handle null/undefined JSON gracefully', () => {
            const filters = [{ field: 'platform', values: ['mysql'] }];
            expect(filtersToSelectedUrns(filters, null)).toEqual([]);
            expect(filtersToSelectedUrns(filters, undefined)).toEqual([]);
        });
    });

    describe('getInitialTabKey', () => {
        it('should return SELECT_ASSETS_TAB_KEY when urn field is in filters', () => {
            const filters = [{ field: URN_FILTER_NAME, values: ['urn:li:dataset:test1'] }];
            expect(getInitialTabKey(filters)).toBe(SELECT_ASSETS_TAB_KEY);
        });

        it('should return BUILD_FILTERS_TAB_KEY when no urn field in filters', () => {
            const filters = [{ field: 'platform', values: ['mysql'] }];
            expect(getInitialTabKey(filters)).toBe(BUILD_FILTERS_TAB_KEY);
        });

        it('should return BUILD_FILTERS_TAB_KEY for empty filters', () => {
            expect(getInitialTabKey([])).toBe(BUILD_FILTERS_TAB_KEY);
        });

        it('should fall back to JSON when filters array is empty', () => {
            const json = JSON.stringify({
                type: 'logical',
                operator: 'or',
                operands: [
                    {
                        type: 'property',
                        property: URN_FILTER_NAME,
                        operator: 'equals',
                        values: ['urn:li:dataset:json1'],
                    },
                ],
            });
            expect(getInitialTabKey([], json)).toBe(SELECT_ASSETS_TAB_KEY);
        });

        it('should return BUILD_FILTERS_TAB_KEY when JSON has no URN field', () => {
            const json = JSON.stringify({
                type: 'logical',
                operator: 'and',
                operands: [
                    {
                        type: 'property',
                        property: 'platform',
                        operator: 'equals',
                        values: ['mysql'],
                    },
                ],
            });
            expect(getInitialTabKey([], json)).toBe(BUILD_FILTERS_TAB_KEY);
        });

        it('should prefer filters array over JSON', () => {
            const filters = [{ field: URN_FILTER_NAME, values: ['urn:li:dataset:from_filters'] }];
            const json = JSON.stringify({
                type: 'logical',
                operator: 'or',
                operands: [
                    {
                        type: 'property',
                        property: 'platform',
                        operator: 'equals',
                        values: ['mysql'],
                    },
                ],
            });
            expect(getInitialTabKey(filters, json)).toBe(SELECT_ASSETS_TAB_KEY);
        });

        it('should handle malformed JSON gracefully', () => {
            const invalidJson = 'not valid json';
            expect(getInitialTabKey([], invalidJson)).toBe(BUILD_FILTERS_TAB_KEY);
        });
    });
});
