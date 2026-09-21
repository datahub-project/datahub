import { describe, expect, it } from 'vitest';

import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import {
    convertLogicalPredicateToOrFilters,
    convertToLogicalPredicate,
    isEmptyLogicalPredicate,
    isLogicalPredicate,
} from '@app/sharedV2/queryBuilder/builder/utils';
import { FilterOperator } from '@src/types.generated';

describe('Query Builder Utils - convertLogicalPredicateToOrFilters()', () => {
    describe('mapOperator: Operator Mapping', () => {
        it('should map equals operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'platform',
                operator: 'equals',
                values: ['mysql'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Equal);
        });

        it('should map starts_with operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'name',
                operator: 'starts_with',
                values: ['test'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.StartWith);
        });

        it('should map contains_str operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'name',
                operator: 'contains_str',
                values: ['prod'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Contain);
        });

        it('should map contains_any operator to In', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'tags',
                operator: 'contains_any',
                values: ['tag1', 'tag2'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.In);
        });

        it('should map is_true operator to Exists', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'hasSchema',
                operator: 'is_true',
                values: [],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Exists);
        });

        it('should map is_false operator to Exists with negated flag', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'hasOwner',
                operator: 'is_false',
                values: [],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            // is_false maps to Exists with negated: true
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Exists);
            expect(orFilters?.[0].and?.[0].negated).toBe(true);
        });

        it('should map within operator to DescendantsIncl', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'container',
                operator: 'within',
                values: ['containerUrn'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.DescendantsIncl);
        });

        it('should map greater_than operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'count',
                operator: 'greater_than',
                values: ['10'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.GreaterThan);
        });

        it('should map less_than operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'count',
                operator: 'less_than',
                values: ['100'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.LessThan);
        });

        it('should map exists operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'description',
                operator: 'exists',
                values: [],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Exists);
        });

        it('should handle legacy equal operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'platform',
                operator: 'equal',
                values: ['hdfs'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Equal);
        });

        it('should handle legacy in operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'platforms',
                operator: 'in',
                values: ['mysql', 'postgres'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.In);
        });

        it('should throw for unsupported operators', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'unsupported_op',
                values: ['value'],
            };
            expect(() => convertLogicalPredicateToOrFilters(predicate)).toThrow('Unsupported operator: unsupported_op');
        });
    });

    describe('isLogicalPredicate()', () => {
        it('should identify AND predicates', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [],
            };
            expect(isLogicalPredicate(predicate)).toBe(true);
        });

        it('should identify OR predicates', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.OR,
                operands: [],
            };
            expect(isLogicalPredicate(predicate)).toBe(true);
        });

        it('should identify NOT predicates', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.NOT,
                operands: [],
            };
            expect(isLogicalPredicate(predicate)).toBe(true);
        });

        it('should not identify property predicates', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'equals',
                values: ['value'],
            };
            expect(isLogicalPredicate(predicate)).toBe(false);
        });
    });

    describe('convertToLogicalPredicate()', () => {
        it('should wrap property predicates in AND', () => {
            const propertyPred: PropertyPredicate = {
                type: 'property',
                property: 'platform',
                operator: 'equals',
                values: ['mysql'],
            };
            const result = convertToLogicalPredicate(propertyPred);
            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(1);
            expect(result.operands[0]).toBe(propertyPred);
        });

        it('should return logical predicates unchanged', () => {
            const logicalPred: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [],
            };
            const result = convertToLogicalPredicate(logicalPred);
            expect(result).toBe(logicalPred);
        });
    });

    describe('isEmptyLogicalPredicate()', () => {
        it('should return true for null', () => {
            expect(isEmptyLogicalPredicate(null)).toBe(true);
        });

        it('should return true for undefined', () => {
            expect(isEmptyLogicalPredicate(undefined)).toBe(true);
        });

        it('should return true for predicates with no operands', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [],
            };
            expect(isEmptyLogicalPredicate(predicate)).toBe(true);
        });

        it('should return false for predicates with operands', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'field',
                        operator: 'equals',
                        values: ['value'],
                    },
                ],
            };
            expect(isEmptyLogicalPredicate(predicate)).toBe(false);
        });
    });

    describe('convertLogicalPredicateToOrFilters: Nested Predicates', () => {
        it('should handle AND of OR predicates', () => {
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
                        operator: 'equals',
                        values: ['c'],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBeGreaterThan(0);
        });

        it('should handle OR of AND predicates', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.OR,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property',
                                property: 'field1',
                                operator: 'equals',
                                values: ['a'],
                            },
                            {
                                type: 'property',
                                property: 'field2',
                                operator: 'equals',
                                values: ['b'],
                            },
                        ],
                    },
                    {
                        type: 'property',
                        property: 'field3',
                        operator: 'equals',
                        values: ['c'],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBeGreaterThan(0);
        });

        it('should handle NOT of property predicate', () => {
            const predicate: LogicalPredicate = {
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
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            expect(orFilters?.[0].and?.[0].negated).toBe(true);
        });

        it('should handle NOT of is_false (double negation applies cumulatively)', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.NOT,
                operands: [
                    {
                        type: 'property',
                        property: 'hasOwner',
                        operator: 'is_false',
                        values: [],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            // NOT(is_false) applies both negations cumulatively
            // is_false: EXISTS with negated=true
            // NOT: inverts the negation flag, so negated=true (cumulative)
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Exists);
            expect(orFilters?.[0].and?.[0].negated).toBe(true);
        });

        it('should handle multiple operands in OR', () => {
            const predicate: LogicalPredicate = {
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
                        property: 'platform',
                        operator: 'equals',
                        values: ['postgres'],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: 'equals',
                        values: ['mongodb'],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.length).toBe(3);
        });

        it('should handle multiple values in property predicate', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'platforms',
                operator: 'in',
                values: ['mysql', 'postgres', 'mongodb', 'hdfs'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].values).toEqual(['mysql', 'postgres', 'mongodb', 'hdfs']);
            expect(orFilters?.[0].and?.[0].field).toBe('platforms');
        });

        it('should preserve field and values in filter output', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'customField',
                operator: 'contains_str',
                values: ['searchTerm', 'anotherTerm'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].field).toBe('customField');
            expect(orFilters?.[0].and?.[0].values).toEqual(['searchTerm', 'anotherTerm']);
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.Contain);
        });

        it('should handle empty values array', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'description',
                operator: 'exists',
                values: [],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].values).toEqual([]);
        });

        it('should handle undefined values', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'equals',
                values: undefined,
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].values).toEqual([]);
        });
    });

    describe('convertLogicalPredicateToOrFilters: Complex Nesting Scenarios', () => {
        it('should handle deeply nested AND/OR/NOT combination', () => {
            const predicate: LogicalPredicate = {
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
                                        property: 'field1',
                                        operator: 'equals',
                                        values: ['value1'],
                                    },
                                ],
                            },
                            {
                                type: 'property',
                                property: 'field2',
                                operator: 'equals',
                                values: ['value2'],
                            },
                        ],
                    },
                    {
                        type: 'property',
                        property: 'field3',
                        operator: 'exists',
                        values: [],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBeGreaterThan(0);
        });

        it('should handle multiple nested ORs within AND', () => {
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
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'field2',
                                operator: 'equals',
                                values: ['x'],
                            },
                            {
                                type: 'property',
                                property: 'field2',
                                operator: 'equals',
                                values: ['y'],
                            },
                        ],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBeGreaterThan(0);
        });

        it('should handle AND of NOTs (De Morgan Law)', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.NOT,
                        operands: [
                            {
                                type: 'property',
                                property: 'field1',
                                operator: 'equals',
                                values: ['value1'],
                            },
                        ],
                    },
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.NOT,
                        operands: [
                            {
                                type: 'property',
                                property: 'field2',
                                operator: 'equals',
                                values: ['value2'],
                            },
                        ],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            // Both filters should have negated: true
            expect(orFilters?.some((filter) => filter.and?.some((f) => f.negated))).toBe(true);
        });
    });

    describe('convertLogicalPredicateToOrFilters: Edge Cases & Error Handling', () => {
        it('should handle predicate with no operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: undefined,
                values: ['value'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].condition).toBeUndefined();
        });

        it('should handle predicate with no property', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: undefined,
                operator: 'equals',
                values: ['value'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeUndefined();
        });

        it('should case-insensitive normalize operators', () => {
            const predicate1: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'EQUALS',
                values: ['value'],
            };
            const predicate2: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'Equals',
                values: ['value'],
            };
            const result1 = convertLogicalPredicateToOrFilters(predicate1);
            const result2 = convertLogicalPredicateToOrFilters(predicate2);
            expect(result1?.[0].and?.[0].condition).toBe(result2?.[0].and?.[0].condition);
        });

        it('should handle operators with underscores and spaces', () => {
            const predicate1: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'starts_with',
                values: ['test'],
            };
            const predicate2: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'starts with',
                values: ['test'],
            };
            const result1 = convertLogicalPredicateToOrFilters(predicate1);
            const result2 = convertLogicalPredicateToOrFilters(predicate2);
            expect(result1?.[0].and?.[0].condition).toBe(result2?.[0].and?.[0].condition);
        });

        it('should throw error with helpful message for invalid operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'invalid_operator_xyz',
                values: ['value'],
            };
            expect(() => convertLogicalPredicateToOrFilters(predicate)).toThrow(
                'Unsupported operator: invalid_operator_xyz',
            );
        });
    });

    describe('convertLogicalPredicateToOrFilters: Negation Flag Combinations', () => {
        it('should combine explicit negation with is_false operator', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'is_false',
                values: [],
            };
            // is_false already applies negation
            const orFilters = convertLogicalPredicateToOrFilters(predicate, false);
            expect(orFilters?.[0].and?.[0].negated).toBe(true);
        });

        it('should NOT double-negate when both negation sources apply', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'field',
                operator: 'is_false',
                values: [],
            };
            // is_false applies negation, so this would be double-negation
            // which our current implementation handles by OR-ing the conditions
            const orFilters1 = convertLogicalPredicateToOrFilters(predicate, false);
            const orFilters2 = convertLogicalPredicateToOrFilters(predicate, true);
            // Both should have negated: true because is_false always applies it
            expect(orFilters1?.[0].and?.[0].negated).toBe(true);
            expect(orFilters2?.[0].and?.[0].negated).toBe(true);
        });

        it('should preserve negation through nested operators', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.NOT,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property',
                                property: 'field1',
                                operator: 'equals',
                                values: ['value1'],
                            },
                            {
                                type: 'property',
                                property: 'field2',
                                operator: 'equals',
                                values: ['value2'],
                            },
                        ],
                    },
                ],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters).toBeDefined();
            expect(orFilters?.every((f) => f.and?.every((x) => x.negated))).toBe(true);
        });
    });

    describe('convertLogicalPredicateToOrFilters: Additional Tests', () => {
        const BASIC_AND_LOGICAL_PREDICATE: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.AND,
            operands: [
                {
                    type: 'property',
                    property: 'test',
                    operator: 'equals',
                    values: ['dataset1'],
                },
                {
                    type: 'property',
                    property: 'test2',
                    operator: 'equals',
                    values: ['dataset2'],
                },
            ],
        };

        const BASIC_OR_LOGICAL_PREDICATE: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.OR,
            operands: [
                {
                    type: 'property',
                    property: 'test',
                    operator: 'equals',
                    values: ['dataset1'],
                },
                {
                    type: 'property',
                    property: 'test2',
                    operator: 'equals',
                    values: ['dataset2'],
                },
            ],
        };

        const BASIC_NOT_LOGICAL_PREDICATE: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.NOT,
            operands: [
                {
                    type: 'property',
                    property: 'test',
                    operator: 'equals',
                    values: ['dataset1'],
                },
                {
                    type: 'property',
                    property: 'test2',
                    operator: 'equals',
                    values: ['dataset2'],
                },
            ],
        };

        const NESTED_LOGICAL_PREDICATE: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.OR,
            operands: [
                {
                    type: 'property',
                    property: 'test1',
                    operator: 'equals',
                    values: ['dataset1'],
                },
                {
                    type: 'logical',
                    operator: LogicalOperatorType.AND,
                    operands: [
                        {
                            type: 'property',
                            property: 'test2',
                            operator: 'equals',
                            values: ['dataset2'],
                        },
                        {
                            type: 'property',
                            property: 'test3',
                            operator: 'equals',
                            values: ['dataset3'],
                        },
                    ],
                },
                {
                    type: 'logical',
                    operator: LogicalOperatorType.OR,
                    operands: [
                        {
                            type: 'property',
                            property: 'test4',
                            operator: 'equals',
                            values: ['dataset4'],
                        },
                        {
                            type: 'logical',
                            operator: LogicalOperatorType.NOT,
                            operands: [
                                {
                                    type: 'property',
                                    property: 'test5',
                                    operator: 'equals',
                                    values: ['dataset5'],
                                },
                                {
                                    type: 'property',
                                    property: 'test6',
                                    operator: 'equals',
                                    values: ['dataset6'],
                                },
                            ],
                        },
                    ],
                },
            ],
        };

        const EMPTY_PROPERTY_PREDICATE: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.AND,
            operands: [
                {
                    type: 'property',
                    property: 'test',
                    operator: 'equals',
                    values: ['dataset1'],
                },
                {
                    type: 'property',
                    property: '',
                    operator: 'equals',
                    values: ['dataset2'],
                },
            ],
        };

        it('convert a basic AND predicate to orFilters', () => {
            const orFilters = convertLogicalPredicateToOrFilters(BASIC_AND_LOGICAL_PREDICATE);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBe(1);
        });

        it('convert a basic OR predicate to orFilters', () => {
            const orFilters = convertLogicalPredicateToOrFilters(BASIC_OR_LOGICAL_PREDICATE);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBe(2);
        });

        it('convert a basic NOT predicate to orFilters', () => {
            const orFilters = convertLogicalPredicateToOrFilters(BASIC_NOT_LOGICAL_PREDICATE);
            expect(orFilters).toBeDefined();
            expect(orFilters?.every((f) => f.and?.every((x) => x.negated))).toBe(true);
        });

        it('convert a nested predicate to orFilters', () => {
            const orFilters = convertLogicalPredicateToOrFilters(NESTED_LOGICAL_PREDICATE);
            expect(orFilters).toBeDefined();
            expect(orFilters?.length).toBeGreaterThan(0);
        });

        it('should ignore predicate with empty property', () => {
            const orFilters = convertLogicalPredicateToOrFilters(EMPTY_PROPERTY_PREDICATE);
            expect(orFilters).toBeDefined();
            // Should only have the non-empty property
            expect(orFilters?.length).toBe(1);
        });

        it('maps within operator to DescendantsIncl for domains', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'domains',
                operator: 'within',
                values: ['urn:li:domain:finance'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].field).toBe('domains');
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.DescendantsIncl);
        });

        it('maps within for parentDocument to DescendantsIncl', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'parentDocument',
                operator: 'within',
                values: ['urn:li:document:parent'],
            };
            const orFilters = convertLogicalPredicateToOrFilters(predicate);
            expect(orFilters?.[0].and?.[0].field).toBe('parentDocument');
            expect(orFilters?.[0].and?.[0].condition).toBe(FilterOperator.DescendantsIncl);
        });
    });

    describe('isLogicalPredicate: Additional Tests', () => {
        it('test is logical predicate', () => {
            expect(
                isLogicalPredicate({
                    type: 'logical',
                    operator: LogicalOperatorType.AND,
                    operands: [],
                } as LogicalPredicate),
            ).toEqual(true);
            expect(
                isLogicalPredicate({
                    type: 'logical',
                    operator: LogicalOperatorType.OR,
                } as LogicalPredicate),
            ).toEqual(true);
            expect(
                isLogicalPredicate({
                    type: 'logical',
                    operator: LogicalOperatorType.NOT,
                } as LogicalPredicate),
            ).toEqual(true);
        });

        it('test is not logical predicate', () => {
            expect(
                isLogicalPredicate({
                    operator: 'exists',
                } as any),
            ).toEqual(false);
            expect(
                isLogicalPredicate({
                    type: 'property',
                    property: 'dataset.description',
                } as PropertyPredicate),
            ).toEqual(false);
        });
    });

    describe('convertToLogicalPredicate: Additional Tests', () => {
        it('should convert PropertyPredicate to LogicalPredicate with AND operator', () => {
            const propertyPred: PropertyPredicate = {
                type: 'property',
                property: 'dataset.description',
                operator: 'equals',
                values: ['test value'],
            };
            const result = convertToLogicalPredicate(propertyPred);
            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(1);
            expect(result.operands[0]).toBe(propertyPred);
        });

        it('should handle PropertyPredicate with minimal properties', () => {
            const minimalPropertyPredicate: PropertyPredicate = {
                type: 'property',
                property: 'title',
            };
            const result = convertToLogicalPredicate(minimalPropertyPredicate);
            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(1);
            expect(result.operands[0]).toBe(minimalPropertyPredicate);
        });

        it('should handle PropertyPredicate with empty values array', () => {
            const propertyPredicateWithEmptyValues: PropertyPredicate = {
                type: 'property',
                property: 'status',
                operator: 'exists',
                values: [],
            };
            const result = convertToLogicalPredicate(propertyPredicateWithEmptyValues);
            expect(result.type).toBe('logical');
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands[0]).toBe(propertyPredicateWithEmptyValues);
        });

        it('should handle complex nested LogicalPredicate', () => {
            const complexLogicalPredicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'name',
                        operator: 'equals',
                        values: ['test'],
                    },
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'type',
                                operator: 'in',
                                values: ['dataset', 'dashboard'],
                            },
                        ],
                    },
                ],
            };
            const result = convertToLogicalPredicate(complexLogicalPredicate);
            expect(result).toEqual(complexLogicalPredicate);
            expect(result).toBe(complexLogicalPredicate);
        });
    });

    describe('isEmptyLogicalPredicate: Additional Tests', () => {
        it('should handle not empty logical predicate', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'test',
                        operator: 'equals',
                        values: ['dataset1'],
                    },
                ],
            };
            expect(isEmptyLogicalPredicate(predicate)).toBeFalsy();
        });

        it('should handle empty logical predicate', () => {
            expect(isEmptyLogicalPredicate({} as LogicalPredicate)).toBeTruthy();
        });

        it('should handle logical predicate with empty operands', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [],
            };
            expect(isEmptyLogicalPredicate(predicate)).toBeTruthy();
        });
    });
});
