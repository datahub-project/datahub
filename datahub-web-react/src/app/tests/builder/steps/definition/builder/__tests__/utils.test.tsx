import { LogicalOperatorType } from '../types';
import {
    convertLogicalPredicateToOrFilters,
    convertLogicalPredicateToTestPredicate,
    convertTestPredicateToLogicalPredicate,
    isLogicalPredicate,
} from '../utils';

const FULL_PROPERTY_PREDICATE = {
    property: 'test',
    operator: 'equals',
    values: ['dataset1'],
};

const PARTIAL_PROPERTY_PREDICATE = {
    property: 'test',
};

const AND_PREDICATE = {
    and: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const TRANSFORMED_AND_PREDICATE = {
    operator: LogicalOperatorType.AND,
    operands: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const OR_PREDICATE = {
    or: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const TRANSFORMED_OR_PREDICATE = {
    operator: LogicalOperatorType.OR,
    operands: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const NOT_PREDICATE_1 = {
    not: {
        ...FULL_PROPERTY_PREDICATE,
    },
};

const TRANSFORMED_NOT_PREDICATE_1 = {
    operator: LogicalOperatorType.NOT,
    operands: [{ ...FULL_PROPERTY_PREDICATE }],
};

const INVERSE_TRANSFORMED_NOT_PREDICATE_1 = {
    not: [{ ...FULL_PROPERTY_PREDICATE }],
};

const NOT_PREDICATE_2 = {
    not: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const TRANSFORMED_NOT_PREDICATE_2 = {
    operator: LogicalOperatorType.NOT,
    operands: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const LIST_PREDICATE = [
    {
        ...FULL_PROPERTY_PREDICATE,
    },
    {
        ...PARTIAL_PROPERTY_PREDICATE,
    },
];

const TRANSFORMED_LIST_PREDICATE = {
    operator: LogicalOperatorType.AND,
    operands: [
        {
            ...FULL_PROPERTY_PREDICATE,
        },
        {
            ...PARTIAL_PROPERTY_PREDICATE,
        },
    ],
};

const COMPLEX_PREDICATE = [
    {
        and: [
            {
                property: 'test1',
                operator: 'equals',
                values: ['value'],
            },
        ],
    },
    {
        or: [
            {
                property: 'test1',
                operator: 'equals',
                values: ['value'],
            },
            {
                not: [
                    {
                        property: 'test1',
                        operator: 'equals',
                        values: ['value'],
                    },
                ],
            },
        ],
    },
    {
        not: {
            and: [
                {
                    property: 'test2',
                    operator: 'equals',
                    values: ['value'],
                },
            ],
        },
    },
    {
        property: 'property',
        operator: 'exists',
    },
];

const TRANSFORMED_COMPLEX_PREDICATE = {
    operator: LogicalOperatorType.AND,
    operands: [
        {
            operator: LogicalOperatorType.AND,
            operands: [
                {
                    property: 'test1',
                    operator: 'equals',
                    values: ['value'],
                },
            ],
        },
        {
            operator: LogicalOperatorType.OR,
            operands: [
                {
                    property: 'test1',
                    operator: 'equals',
                    values: ['value'],
                },
                {
                    operator: LogicalOperatorType.NOT,
                    operands: [
                        {
                            property: 'test1',
                            operator: 'equals',
                            values: ['value'],
                        },
                    ],
                },
            ],
        },
        {
            operator: LogicalOperatorType.NOT,
            operands: [
                {
                    operator: LogicalOperatorType.AND,
                    operands: [
                        {
                            property: 'test2',
                            operator: 'equals',
                            values: ['value'],
                        },
                    ],
                },
            ],
        },
        {
            property: 'property',
            operator: 'exists',
        },
    ],
};

const INVERSE_TRANSFORMED_COMPLEX_PREDICATE = [
    {
        and: [
            {
                property: 'test1',
                operator: 'equals',
                values: ['value'],
            },
        ],
    },
    {
        or: [
            {
                property: 'test1',
                operator: 'equals',
                values: ['value'],
            },
            {
                not: [
                    {
                        property: 'test1',
                        operator: 'equals',
                        values: ['value'],
                    },
                ],
            },
        ],
    },
    {
        not: [
            {
                and: [
                    {
                        property: 'test2',
                        operator: 'equals',
                        values: ['value'],
                    },
                ],
            },
        ],
    },
    {
        property: 'property',
        operator: 'exists',
    },
];

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

    describe('convertTestPredicateToLogicalPredicate', () => {
        it('convert property predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(FULL_PROPERTY_PREDICATE)).toEqual(FULL_PROPERTY_PREDICATE);
        });
        it('convert partial property predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(PARTIAL_PROPERTY_PREDICATE)).toEqual(
                PARTIAL_PROPERTY_PREDICATE,
            );
        });
        it('convert AND predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(AND_PREDICATE)).toEqual(TRANSFORMED_AND_PREDICATE);
        });
        it('convert OR predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(OR_PREDICATE)).toEqual(TRANSFORMED_OR_PREDICATE);
        });
        it('convert NOT predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(NOT_PREDICATE_1)).toEqual(TRANSFORMED_NOT_PREDICATE_1);
            expect(convertTestPredicateToLogicalPredicate(NOT_PREDICATE_2)).toEqual(TRANSFORMED_NOT_PREDICATE_2);
        });
        it('convert list predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(LIST_PREDICATE)).toEqual(TRANSFORMED_LIST_PREDICATE);
        });
        it('convert complex predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(COMPLEX_PREDICATE)).toEqual(TRANSFORMED_COMPLEX_PREDICATE);
        });
    });

    describe('convertLogicalPredicateToTestPredicate', () => {
        it('convert property predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(FULL_PROPERTY_PREDICATE)).toEqual(FULL_PROPERTY_PREDICATE);
        });
        it('convert partial property predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(PARTIAL_PROPERTY_PREDICATE)).toEqual(
                PARTIAL_PROPERTY_PREDICATE,
            );
        });
        it('convert AND predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_AND_PREDICATE)).toEqual(AND_PREDICATE);
        });
        it('convert OR predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_OR_PREDICATE)).toEqual(OR_PREDICATE);
        });
        it('convert NOT predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_NOT_PREDICATE_1)).toEqual(
                INVERSE_TRANSFORMED_NOT_PREDICATE_1,
            );
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_NOT_PREDICATE_2)).toEqual(NOT_PREDICATE_2);
        });
        it('convert list predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_LIST_PREDICATE)).toEqual({
                and: LIST_PREDICATE,
            });
        });
        it('convert complex predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_COMPLEX_PREDICATE)).toEqual({
                and: INVERSE_TRANSFORMED_COMPLEX_PREDICATE,
            });
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
