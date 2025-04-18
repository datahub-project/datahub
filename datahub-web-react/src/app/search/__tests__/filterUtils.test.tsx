import { UnionType } from '../utils/constants';
import { mergeFilterSets } from '../utils/filterUtils';

describe('filterUtils', () => {
    describe('mergeFilterSets', () => {
        it('merge conjunction and conjunction', () => {
            const conjunction1 = {
                unionType: UnionType.AND,
                filters: [
                    {
                        field: 'a',
                        values: ['1', '2'],
                    },
                    {
                        field: 'b',
                        values: ['1', '2'],
                    },
                ],
            };
            const conjunction2 = {
                unionType: UnionType.AND,
                filters: [
                    {
                        field: 'c',
                        values: ['1', '2'],
                    },
                    {
                        field: 'd',
                        values: ['1', '2'],
                    },
                ],
            };
            const expectedResult = [
                {
                    and: [
                        {
                            field: 'a',
                            values: ['1', '2'],
                        },
                        {
                            field: 'b',
                            values: ['1', '2'],
                        },
                        {
                            field: 'c',
                            values: ['1', '2'],
                        },
                        {
                            field: 'd',
                            values: ['1', '2'],
                        },
                    ],
                },
            ];
            expect(mergeFilterSets(conjunction1, conjunction2)).toEqual(expectedResult);
        });
        it('merge conjunction and disjunction', () => {
            const conjunction = {
                unionType: UnionType.AND,
                filters: [
                    {
                        field: 'a',
                        values: ['1', '2'],
                    },
                    {
                        field: 'b',
                        values: ['1', '2'],
                    },
                ],
            };
            const disjunction = {
                unionType: UnionType.OR,
                filters: [
                    {
                        field: 'c',
                        values: ['1', '2'],
                    },
                    {
                        field: 'd',
                        values: ['1', '2'],
                    },
                ],
            };
            const expectedResult = [
                {
                    and: [
                        {
                            field: 'c',
                            values: ['1', '2'],
                        },
                        {
                            field: 'a',
                            values: ['1', '2'],
                        },
                        {
                            field: 'b',
                            values: ['1', '2'],
                        },
                    ],
                },
                {
                    and: [
                        {
                            field: 'd',
                            values: ['1', '2'],
                        },
                        {
                            field: 'a',
                            values: ['1', '2'],
                        },
                        {
                            field: 'b',
                            values: ['1', '2'],
                        },
                    ],
                },
            ];
            expect(mergeFilterSets(conjunction, disjunction)).toEqual(expectedResult);
        });
        it('merge disjunction and disjunction', () => {
            const disjunction1 = {
                unionType: UnionType.OR,
                filters: [
                    {
                        field: 'a',
                        values: ['1', '2'],
                    },
                    {
                        field: 'b',
                        values: ['1', '2'],
                    },
                ],
            };
            const disjunction2 = {
                unionType: UnionType.OR,
                filters: [
                    {
                        field: 'c',
                        values: ['1', '2'],
                    },
                    {
                        field: 'd',
                        values: ['1', '2'],
                    },
                ],
            };
            const expectedResult = [
                {
                    and: [
                        {
                            field: 'a',
                            values: ['1', '2'],
                        },
                        {
                            field: 'c',
                            values: ['1', '2'],
                        },
                    ],
                },
                {
                    and: [
                        {
                            field: 'a',
                            values: ['1', '2'],
                        },
                        {
                            field: 'd',
                            values: ['1', '2'],
                        },
                    ],
                },
                {
                    and: [
                        {
                            field: 'b',
                            values: ['1', '2'],
                        },
                        {
                            field: 'c',
                            values: ['1', '2'],
                        },
                    ],
                },
                {
                    and: [
                        {
                            field: 'b',
                            values: ['1', '2'],
                        },
                        {
                            field: 'd',
                            values: ['1', '2'],
                        },
                    ],
                },
            ];
            expect(mergeFilterSets(disjunction1, disjunction2)).toEqual(expectedResult);
        });
        it('bad arguments', () => {
            expect(
                mergeFilterSets(
                    {
                        unionType: UnionType.OR,
                        filters: [{ field: 'field', values: [] }],
                    },
                    null!,
                ),
            ).toEqual([]);
            expect(
                mergeFilterSets(null!, {
                    unionType: UnionType.OR,
                    filters: [{ field: 'field', values: [] }],
                }),
            ).toEqual([]);
            expect(mergeFilterSets(null!, null!)).toEqual([]);
        });
    });
});
