import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters, isLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';

import { AndFilterInput, FilterOperator } from '@types';

describe('utils', () => {
    describe('isLogicalPredicate', () => {
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

    const BASIC_AND_OR_FILTERS: AndFilterInput[] = [
        {
            and: [
                { field: 'test', condition: FilterOperator.Equal, values: ['dataset1'] },
                { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] },
            ],
        },
    ];

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

    const BASIC_OR_OR_FILTERS: AndFilterInput[] = [
        {
            and: [{ field: 'test', condition: FilterOperator.Equal, values: ['dataset1'] }],
        },
        {
            and: [{ field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] }],
        },
    ];

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

    const BASIC_NOT_OR_FILTERS: AndFilterInput[] = [
        {
            and: [
                { field: 'test', condition: FilterOperator.Equal, values: ['dataset1'], negated: true },
                { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'], negated: true },
            ],
        },
    ];

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

    const NESTED_OR_FILTERS: AndFilterInput[] = [
        {
            and: [{ field: 'test1', condition: FilterOperator.Equal, values: ['dataset1'] }],
        },
        {
            and: [
                { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] },
                { field: 'test3', condition: FilterOperator.Equal, values: ['dataset3'] },
            ],
        },
        {
            and: [{ field: 'test4', condition: FilterOperator.Equal, values: ['dataset4'] }],
        },
        {
            and: [
                { field: 'test5', condition: FilterOperator.Equal, values: ['dataset5'], negated: true },
                { field: 'test6', condition: FilterOperator.Equal, values: ['dataset6'], negated: true },
            ],
        },
    ];

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

    const EMPTY_PROPERTY_OR_FILTERS: AndFilterInput[] = [
        {
            and: [{ field: 'test', condition: FilterOperator.Equal, values: ['dataset1'] }],
        },
    ];

    const EMPTY_LOGICAL_PREDICATE: LogicalPredicate = {} as any;

    const LOGICAL_PREDICATE_WITH_UNKNOWN_OPERATION: LogicalPredicate = {
        type: 'logical',
        operator: 'UNKNOWN',
    } as any;

    describe('convertLogicalPredicateToOrFilters', () => {
        it('convert a basic AND predicate to orFilters', () => {
            expect(convertLogicalPredicateToOrFilters(BASIC_AND_LOGICAL_PREDICATE)).toEqual(BASIC_AND_OR_FILTERS);
        });

        it('convert a basic OR predicate to orFilters', () => {
            expect(convertLogicalPredicateToOrFilters(BASIC_OR_LOGICAL_PREDICATE)).toEqual(BASIC_OR_OR_FILTERS);
        });

        it('convert a basic NOT predicate to orFilters', () => {
            expect(convertLogicalPredicateToOrFilters(BASIC_NOT_LOGICAL_PREDICATE)).toEqual(BASIC_NOT_OR_FILTERS);
        });
        it('convert a nested predicate to orFilters', () => {
            expect(convertLogicalPredicateToOrFilters(NESTED_LOGICAL_PREDICATE)).toEqual(NESTED_OR_FILTERS);
        });
        it('should ignore predicate with empty property', () => {
            expect(convertLogicalPredicateToOrFilters(EMPTY_PROPERTY_PREDICATE)).toEqual(EMPTY_PROPERTY_OR_FILTERS);
        });
        it('should ignore empty logical predicate', () => {
            expect(convertLogicalPredicateToOrFilters(EMPTY_LOGICAL_PREDICATE)).toEqual(undefined);
        });
        it('should ignore logical predicate with unknown operator', () => {
            expect(convertLogicalPredicateToOrFilters(LOGICAL_PREDICATE_WITH_UNKNOWN_OPERATION)).toEqual(undefined);
        });
    });
});
