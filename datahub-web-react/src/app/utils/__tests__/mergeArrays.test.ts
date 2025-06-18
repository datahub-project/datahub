import { MergeStrategy, mergeArrays } from '@app/utils/mergeArrays';

describe('mergeArrays', () => {
    const mockData1 = { id: 1, name: 'Alice' };
    const mockData2 = { id: 2, name: 'Bob' };
    const mockData3 = { id: 3, name: 'Charlie' };
    const mockData4 = { id: 2, name: 'Updated Bob' };

    const keyGetter = (item: { id: number }) => item.id;

    describe('MergeStrategy.preferAItems', () => {
        it('should keep A items and add only new B items', () => {
            const result = mergeArrays(
                [mockData1, mockData2],
                [mockData4, mockData3],
                keyGetter,
                MergeStrategy.preferAItems,
            );

            expect(result).toEqual([mockData1, mockData2, mockData3]);
        });

        it('should use default keyGetter if not provided', () => {
            const result = mergeArrays([1, 2, 3], [3, 4, 5]);

            expect(result).toEqual([1, 2, 3, 4, 5]);
        });
    });

    describe('MergeStrategy.preferBItems', () => {
        it('should keep B items and add only new A items', () => {
            const result = mergeArrays(
                [mockData1, mockData2],
                [mockData4, mockData3],
                keyGetter,
                MergeStrategy.preferBItems,
            );

            expect(result).toEqual([mockData1, mockData4, mockData3]);
        });

        it('should respect default strategy when unknown strategy is passed', () => {
            const result = mergeArrays(
                [mockData1, mockData2],
                [mockData4, mockData3],
                keyGetter,
                // @ts-expect-error - simulate invalid input
                'unknownStrategy',
            );

            expect(result).toEqual([mockData1, mockData4, mockData3]);
        });
    });

    describe('Edge Cases', () => {
        it('should handle empty arrays correctly', () => {
            expect(mergeArrays([], [], keyGetter)).toEqual([]);
            expect(mergeArrays([mockData1], [], keyGetter)).toEqual([mockData1]);
            expect(mergeArrays([], [mockData1], keyGetter)).toEqual([mockData1]);
        });

        it('should deduplicate based on custom keyGetter', () => {
            const result = mergeArrays([{ id: 1 }, { id: 2 }], [{ id: 2 }, { id: 3 }], keyGetter);

            expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
        });

        it('should work with primitive values', () => {
            const result = mergeArrays([1, 2, 3], [3, 4, 5]);

            expect(result).toEqual([1, 2, 3, 4, 5]);
        });

        it('should allow custom keyGetters like string or composite keys', () => {
            const arrayWithNames = [{ name: 'apple' }, { name: 'banana' }];
            const arrayWithMoreBananas = [{ name: 'banana' }, { name: 'cherry' }];

            const result = mergeArrays(
                arrayWithNames,
                arrayWithMoreBananas,
                (item) => item.name,
                MergeStrategy.preferAItems,
            );

            expect(result).toEqual([{ name: 'apple' }, { name: 'banana' }, { name: 'cherry' }]);
        });
    });
});
