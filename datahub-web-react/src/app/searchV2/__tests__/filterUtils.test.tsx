import { FilterOperator } from '@src/types.generated';
import { UnionType } from '../utils/constants';
import { combineOrFilters, mergeFilterSets } from '../utils/filterUtils';

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

    describe('combineOrFilters', () => {
        it('should combine two basic orFilters', () => {
            const orFilter1 = [
                {
                    and: [
                        { field: 'test1', condition: FilterOperator.Equal, values: ['dataset1'] },
                        { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] },
                    ],
                },
            ];
            const orFilter2 = [
                {
                    and: [
                        { field: 'test3', condition: FilterOperator.Equal, values: ['dataset3'] },
                        { field: 'test4', condition: FilterOperator.Equal, values: ['dataset4'] },
                    ],
                },
            ];
            const expectedOrFilter = [
                {
                    and: [
                        { field: 'test1', condition: FilterOperator.Equal, values: ['dataset1'] },
                        { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] },
                        { field: 'test3', condition: FilterOperator.Equal, values: ['dataset3'] },
                        { field: 'test4', condition: FilterOperator.Equal, values: ['dataset4'] },
                    ],
                },
            ];

            expect(combineOrFilters(orFilter1, orFilter2)).toEqual(expectedOrFilter);
        });
        it('should combine more complex orFilters', () => {
            const orFilter1 = [
                {
                    and: [{ field: 'test1', condition: FilterOperator.Equal, values: ['dataset1'] }],
                },
                {
                    and: [{ field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] }],
                },
            ];
            const orFilter2 = [
                {
                    and: [
                        { field: 'test3', condition: FilterOperator.Equal, values: ['dataset3'] },
                        { field: 'test4', condition: FilterOperator.Equal, values: ['dataset4'] },
                    ],
                },
                {
                    and: [{ field: 'test5', condition: FilterOperator.Equal, values: ['dataset5'] }],
                },
            ];
            const expectedOrFilter = [
                {
                    and: [
                        { field: 'test1', condition: FilterOperator.Equal, values: ['dataset1'] },
                        { field: 'test3', condition: FilterOperator.Equal, values: ['dataset3'] },
                        { field: 'test4', condition: FilterOperator.Equal, values: ['dataset4'] },
                    ],
                },
                {
                    and: [
                        { field: 'test1', condition: FilterOperator.Equal, values: ['dataset1'] },
                        { field: 'test5', condition: FilterOperator.Equal, values: ['dataset5'] },
                    ],
                },
                {
                    and: [
                        { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] },
                        { field: 'test3', condition: FilterOperator.Equal, values: ['dataset3'] },
                        { field: 'test4', condition: FilterOperator.Equal, values: ['dataset4'] },
                    ],
                },
                {
                    and: [
                        { field: 'test2', condition: FilterOperator.Equal, values: ['dataset2'] },
                        { field: 'test5', condition: FilterOperator.Equal, values: ['dataset5'] },
                    ],
                },
            ];

            expect(combineOrFilters(orFilter1, orFilter2)).toEqual(expectedOrFilter);
        });
    });
});
