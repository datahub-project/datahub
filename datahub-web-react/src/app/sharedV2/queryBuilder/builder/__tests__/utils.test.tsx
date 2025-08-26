import { LogicalOperatorType } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters, isLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';

describe('utils', () => {
    describe('isLogicalPredicate', () => {
        it('test is logical predicate', () => {
            expect(
                isLogicalPredicate({
                    operator: LogicalOperatorType.AND,
                    operands: [],
                }),
            ).toEqual(true);
            expect(
                isLogicalPredicate({
                    operator: LogicalOperatorType.OR,
                }),
            ).toEqual(true);
            expect(
                isLogicalPredicate({
                    operator: LogicalOperatorType.NOT,
                }),
            ).toEqual(true);
        });
        it('test is not logical predicate', () => {
            expect(
                isLogicalPredicate({
                    operator: 'exists',
                }),
            ).toEqual(false);
            expect(
                isLogicalPredicate({
                    property: 'dataset.description',
                }),
            ).toEqual(false);
        });
    });

    const BASIC_AND_LOGICAL_PREDICATE = {
        operator: LogicalOperatorType.AND,
        operands: [
            {
                property: 'test',
                operator: 'equals',
                values: ['dataset1'],
            },
            {
                property: 'test2',
                operator: 'equals',
                values: ['dataset2'],
            },
        ],
    };

    const BASIC_AND_OR_FILTERS = [
        {
            and: [
                { field: 'test', condition: 'EQUAL', values: ['dataset1'] },
                { field: 'test2', condition: 'EQUAL', values: ['dataset2'] },
            ],
        },
    ];

    const BASIC_OR_LOGICAL_PREDICATE = {
        operator: LogicalOperatorType.OR,
        operands: [
            {
                property: 'test',
                operator: 'equals',
                values: ['dataset1'],
            },
            {
                property: 'test2',
                operator: 'equals',
                values: ['dataset2'],
            },
        ],
    };

    const BASIC_OR_OR_FILTERS = [
        {
            and: [{ field: 'test', condition: 'EQUAL', values: ['dataset1'] }],
        },
        {
            and: [{ field: 'test2', condition: 'EQUAL', values: ['dataset2'] }],
        },
    ];

    const BASIC_NOT_LOGICAL_PREDICATE = {
        operator: LogicalOperatorType.NOT,
        operands: [
            {
                property: 'test',
                operator: 'equals',
                values: ['dataset1'],
            },
            {
                property: 'test2',
                operator: 'equals',
                values: ['dataset2'],
            },
        ],
    };

    const BASIC_NOT_OR_FILTERS = [
        {
            and: [
                { field: 'test', condition: 'EQUAL', values: ['dataset1'], negated: true },
                { field: 'test2', condition: 'EQUAL', values: ['dataset2'], negated: true },
            ],
        },
    ];

    const NESTED_LOGICAL_PREDICATE = {
        operator: LogicalOperatorType.OR,
        operands: [
            {
                property: 'test1',
                operator: 'equals',
                values: ['dataset1'],
            },
            {
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        property: 'test2',
                        operator: 'equals',
                        values: ['dataset2'],
                    },
                    {
                        property: 'test3',
                        operator: 'equals',
                        values: ['dataset3'],
                    },
                ],
            },
            {
                operator: LogicalOperatorType.OR,
                operands: [
                    {
                        property: 'test4',
                        operator: 'equals',
                        values: ['dataset4'],
                    },
                    {
                        operator: LogicalOperatorType.NOT,
                        operands: [
                            {
                                property: 'test5',
                                operator: 'equals',
                                values: ['dataset5'],
                            },
                            {
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

    const NESTED_OR_FILTERS = [
        {
            and: [{ field: 'test1', condition: 'EQUAL', values: ['dataset1'] }],
        },
        {
            and: [
                { field: 'test2', condition: 'EQUAL', values: ['dataset2'] },
                { field: 'test3', condition: 'EQUAL', values: ['dataset3'] },
            ],
        },
        {
            and: [{ field: 'test4', condition: 'EQUAL', values: ['dataset4'] }],
        },
        {
            and: [
                { field: 'test5', condition: 'EQUAL', values: ['dataset5'], negated: true },
                { field: 'test6', condition: 'EQUAL', values: ['dataset6'], negated: true },
            ],
        },
    ];

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
    });
});
