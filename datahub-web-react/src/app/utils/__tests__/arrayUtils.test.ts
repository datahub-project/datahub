import { mergeArraysOfObjects } from '@app/utils/arrayUtils';

// Adjust the import path as needed

describe('mergeArraysOfObjects', () => {
    // Test with simple objects using id property
    it('should merge arrays and replace objects with same id', () => {
        const arrayA = [
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'Charlie' },
        ];

        const arrayB = [
            { id: 2, name: 'Bob Updated' },
            { id: 4, name: 'David' },
        ];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([
            { id: 1, name: 'Alice' },
            { id: 3, name: 'Charlie' },
            { id: 2, name: 'Bob Updated' },
            { id: 4, name: 'David' },
        ]);
    });

    // Test with string keys
    it('should work with string keys', () => {
        const arrayA = [
            { email: 'alice@example.com', name: 'Alice' },
            { email: 'bob@example.com', name: 'Bob' },
        ];

        const arrayB = [
            { email: 'bob@example.com', name: 'Bob Updated' },
            { email: 'charlie@example.com', name: 'Charlie' },
        ];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.email);

        expect(result).toEqual([
            { email: 'alice@example.com', name: 'Alice' },
            { email: 'bob@example.com', name: 'Bob Updated' },
            { email: 'charlie@example.com', name: 'Charlie' },
        ]);
    });

    // Test when arrayB is empty
    it('should return arrayA when arrayB is empty', () => {
        const arrayA = [{ id: 1, name: 'Alice' }];
        const arrayB: Array<{ id: number; name: string }> = [];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([{ id: 1, name: 'Alice' }]);
    });

    // Test when arrayA is empty
    it('should return arrayB when arrayA is empty', () => {
        const arrayA: Array<{ id: number; name: string }> = [];
        const arrayB = [{ id: 1, name: 'Bob' }];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([{ id: 1, name: 'Bob' }]);
    });

    // Test when both arrays are empty
    it('should return empty array when both arrays are empty', () => {
        const arrayA: Array<any> = [];
        const arrayB: Array<any> = [];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([]);
    });

    // Test when there are no overlapping keys
    it('should concatenate arrays when there are no overlapping keys', () => {
        const arrayA = [{ id: 1, name: 'Alice' }];
        const arrayB = [{ id: 2, name: 'Bob' }];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
        ]);
    });

    // Test when all keys overlap (arrayA should be completely replaced)
    it('should replace all items when all keys overlap', () => {
        const arrayA = [
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
        ];

        const arrayB = [
            { id: 1, name: 'Alice Updated' },
            { id: 2, name: 'Bob Updated' },
        ];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([
            { id: 1, name: 'Alice Updated' },
            { id: 2, name: 'Bob Updated' },
        ]);
    });

    // Test with complex key getter function
    it('should work with complex key getter functions', () => {
        const arrayA = [
            { firstName: 'John', lastName: 'Doe', age: 30 },
            { firstName: 'Jane', lastName: 'Smith', age: 25 },
        ];

        const arrayB = [
            { firstName: 'John', lastName: 'Doe', age: 31 }, // Same person, different age
            { firstName: 'Bob', lastName: 'Johnson', age: 40 },
        ];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => `${item.firstName}-${item.lastName}`);

        expect(result).toEqual([
            { firstName: 'Jane', lastName: 'Smith', age: 25 },
            { firstName: 'John', lastName: 'Doe', age: 31 },
            { firstName: 'Bob', lastName: 'Johnson', age: 40 },
        ]);
    });

    // Test with primitive values (not objects)
    it('should work with arrays of primitive values', () => {
        const arrayA = [1, 2, 3, 4];
        const arrayB = [3, 4, 5, 6];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item);

        expect(result).toEqual([1, 2, 3, 4, 5, 6]);
    });

    // Test with duplicate values in arrayB
    it('should handle duplicates in arrayB correctly', () => {
        const arrayA = [{ id: 1, name: 'Alice' }];
        const arrayB = [
            { id: 2, name: 'Bob' },
            { id: 2, name: 'Bob Duplicate' },
        ];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 2, name: 'Bob Duplicate' },
        ]);
    });

    // Test with null and undefined values
    it('should handle null and undefined values in key getter', () => {
        const arrayA = [
            { id: 1, name: 'Alice' },
            { id: null, name: 'Null User' },
        ];

        const arrayB = [
            { id: null, name: 'Null User Updated' },
            { id: 2, name: 'Bob' },
        ];

        const result = mergeArraysOfObjects(arrayA, arrayB, (item) => item.id);

        expect(result).toEqual([
            { id: 1, name: 'Alice' },
            { id: null, name: 'Null User Updated' },
            { id: 2, name: 'Bob' },
        ]);
    });
});
