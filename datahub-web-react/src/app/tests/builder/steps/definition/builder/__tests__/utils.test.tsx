import {
    LogicalOperatorType,
    LogicalPredicate,
    PropertyPredicate,
} from '@app/tests/builder/steps/definition/builder/types';
import {
    convertLogicalPredicateToOrFilters,
    convertLogicalPredicateToTestPredicate,
    convertTestPredicateToLogicalPredicate,
    isLogicalPredicate,
} from '@app/tests/builder/steps/definition/builder/utils';
import { AndPredicate, NotPredicate, OrPredicate, Predicate, TestPredicate } from '@app/tests/types';

const convertPropertyToTestPredicate = (predicate: PropertyPredicate): Predicate => {
    return {
        property: predicate.property ?? '',
        operator: predicate.operator,
        values: predicate.values,
    };
};
const deepConvertPropertyToTestPredicate = (
    predicate,
): AndPredicate | OrPredicate | NotPredicate | TestPredicate[] | Predicate => {
    if (predicate.property) {
        return convertPropertyToTestPredicate(predicate);
    }
    if (predicate.and) {
        return {
            and: predicate.and.map(deepConvertPropertyToTestPredicate),
        };
    }
    if (predicate.or) {
        return {
            or: predicate.or.map(deepConvertPropertyToTestPredicate),
        };
    }
    if (predicate.not) {
        return {
            not: deepConvertPropertyToTestPredicate(predicate.not),
        };
    }
    if (Array.isArray(predicate)) {
        return predicate.map(deepConvertPropertyToTestPredicate);
    }
    throw new Error('Invalid predicate');
};

const FULL_PROPERTY_PREDICATE: PropertyPredicate = {
    type: 'property',
    property: 'test',
    operator: 'equals',
    values: ['dataset1'],
};

const PARTIAL_PROPERTY_PREDICATE: PropertyPredicate = {
    type: 'property',
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

const TRANSFORMED_AND_PREDICATE: LogicalPredicate = {
    type: 'logical',
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

const TRANSFORMED_OR_PREDICATE: LogicalPredicate = {
    type: 'logical',
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

const TRANSFORMED_NOT_PREDICATE_1: LogicalPredicate = {
    type: 'logical',
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

const TRANSFORMED_NOT_PREDICATE_2: LogicalPredicate = {
    type: 'logical',
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

const TRANSFORMED_LIST_PREDICATE: LogicalPredicate = {
    type: 'logical',
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

const TRANSFORMED_COMPLEX_PREDICATE: LogicalPredicate = {
    type: 'logical',
    operator: LogicalOperatorType.AND,
    operands: [
        {
            type: 'logical',
            operator: LogicalOperatorType.AND,
            operands: [
                {
                    type: 'property',
                    property: 'test1',
                    operator: 'equals',
                    values: ['value'],
                },
            ],
        },
        {
            type: 'logical',
            operator: LogicalOperatorType.OR,
            operands: [
                {
                    type: 'property',
                    property: 'test1',
                    operator: 'equals',
                    values: ['value'],
                },
                {
                    type: 'logical',
                    operator: LogicalOperatorType.NOT,
                    operands: [
                        {
                            type: 'property',
                            property: 'test1',
                            operator: 'equals',
                            values: ['value'],
                        },
                    ],
                },
            ],
        },
        {
            type: 'logical',
            operator: LogicalOperatorType.NOT,
            operands: [
                {
                    type: 'logical',
                    operator: LogicalOperatorType.AND,
                    operands: [
                        {
                            type: 'property',
                            property: 'test2',
                            operator: 'equals',
                            values: ['value'],
                        },
                    ],
                },
            ],
        },
        {
            type: 'property',
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
                    type: 'logical',
                    operator: LogicalOperatorType.AND,
                    operands: [],
                }),
            ).toEqual(true);
            expect(
                isLogicalPredicate({
                    type: 'logical',
                    operator: LogicalOperatorType.OR,
                    operands: [],
                }),
            ).toEqual(true);
            expect(
                isLogicalPredicate({
                    type: 'logical',
                    operator: LogicalOperatorType.NOT,
                    operands: [],
                }),
            ).toEqual(true);
        });
        it('test is not logical predicate', () => {
            expect(
                isLogicalPredicate({
                    type: 'property',
                    operator: 'exists',
                }),
            ).toEqual(false);
            expect(
                isLogicalPredicate({
                    type: 'property',
                    property: 'dataset.description',
                }),
            ).toEqual(false);
        });
    });

    describe('convertTestPredicateToLogicalPredicate', () => {
        it('convert property predicate', () => {
            expect(
                convertTestPredicateToLogicalPredicate(convertPropertyToTestPredicate(FULL_PROPERTY_PREDICATE)),
            ).toEqual(FULL_PROPERTY_PREDICATE);
        });
        it('convert partial property predicate', () => {
            expect(
                convertTestPredicateToLogicalPredicate(convertPropertyToTestPredicate(PARTIAL_PROPERTY_PREDICATE)),
            ).toEqual(PARTIAL_PROPERTY_PREDICATE);
        });
        it('convert AND predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(deepConvertPropertyToTestPredicate(AND_PREDICATE))).toEqual(
                TRANSFORMED_AND_PREDICATE,
            );
        });
        it('convert OR predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(deepConvertPropertyToTestPredicate(OR_PREDICATE))).toEqual(
                TRANSFORMED_OR_PREDICATE,
            );
        });
        it('convert NOT predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(deepConvertPropertyToTestPredicate(NOT_PREDICATE_1))).toEqual(
                TRANSFORMED_NOT_PREDICATE_1,
            );
            expect(convertTestPredicateToLogicalPredicate(deepConvertPropertyToTestPredicate(NOT_PREDICATE_2))).toEqual(
                TRANSFORMED_NOT_PREDICATE_2,
            );
        });
        it('convert list predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(deepConvertPropertyToTestPredicate(LIST_PREDICATE))).toEqual(
                TRANSFORMED_LIST_PREDICATE,
            );
        });
        it('convert complex predicate', () => {
            expect(convertTestPredicateToLogicalPredicate(COMPLEX_PREDICATE)).toEqual(TRANSFORMED_COMPLEX_PREDICATE);
        });
    });

    describe('convertLogicalPredicateToTestPredicate', () => {
        it('convert property predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(FULL_PROPERTY_PREDICATE)).toEqual({
                property: FULL_PROPERTY_PREDICATE.property,
                operator: FULL_PROPERTY_PREDICATE.operator,
                values: FULL_PROPERTY_PREDICATE.values,
            });
        });
        it('convert partial property predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(PARTIAL_PROPERTY_PREDICATE)).toEqual({
                property: PARTIAL_PROPERTY_PREDICATE.property,
                operator: PARTIAL_PROPERTY_PREDICATE.operator,
                values: PARTIAL_PROPERTY_PREDICATE.values,
            });
        });
        it('convert AND predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_AND_PREDICATE)).toEqual(
                deepConvertPropertyToTestPredicate(AND_PREDICATE),
            );
        });
        it('convert OR predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_OR_PREDICATE)).toEqual(
                deepConvertPropertyToTestPredicate(OR_PREDICATE),
            );
        });
        it('convert NOT predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_NOT_PREDICATE_1)).toEqual(
                deepConvertPropertyToTestPredicate(INVERSE_TRANSFORMED_NOT_PREDICATE_1),
            );
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_NOT_PREDICATE_2)).toEqual(
                deepConvertPropertyToTestPredicate(NOT_PREDICATE_2),
            );
        });
        it('convert list predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_LIST_PREDICATE)).toEqual({
                and: deepConvertPropertyToTestPredicate(LIST_PREDICATE),
            });
        });
        it('convert complex predicate', () => {
            expect(convertLogicalPredicateToTestPredicate(TRANSFORMED_COMPLEX_PREDICATE)).toEqual({
                and: INVERSE_TRANSFORMED_COMPLEX_PREDICATE,
            });
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

    const BASIC_AND_OR_FILTERS = [
        {
            and: [
                { field: 'test', condition: 'EQUAL', values: ['dataset1'] },
                { field: 'test2', condition: 'EQUAL', values: ['dataset2'] },
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

    const BASIC_OR_OR_FILTERS = [
        {
            and: [{ field: 'test', condition: 'EQUAL', values: ['dataset1'] }],
        },
        {
            and: [{ field: 'test2', condition: 'EQUAL', values: ['dataset2'] }],
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

    const BASIC_NOT_OR_FILTERS = [
        {
            and: [
                { field: 'test', condition: 'EQUAL', values: ['dataset1'], negated: true },
                { field: 'test2', condition: 'EQUAL', values: ['dataset2'], negated: true },
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
