import { SelectOption } from '../types';
import { getChainOfParents } from '../utils';

describe('getChainOfParents', () => {
    const options: SelectOption[] = [
        { value: 'child', label: 'child', parentValue: 'parent' },
        { value: 'parent', label: 'parent', parentValue: 'grandparent' },
        { value: 'grandparent', label: 'grandparent' },
        { value: 'orphan', label: 'orphan', parentValue: 'nonexistent' },
    ];

    it('should return empty array when option is undefined', () => {
        const result = getChainOfParents(undefined, options);
        expect(result).toEqual([]);
    });

    it('should return the option itself if it has no parent', () => {
        const grandparent = options.find((o) => o.value === 'grandparent');
        const result = getChainOfParents(grandparent, options);
        expect(result).toEqual([grandparent]);
    });

    it('should return chain of parents for a child with multiple ancestors', () => {
        const child = options.find((o) => o.value === 'child');
        const parent = options.find((o) => o.value === 'parent');
        const grandparent = options.find((o) => o.value === 'grandparent');
        const result = getChainOfParents(child, options);
        expect(result).toEqual([child, parent, grandparent]);
    });

    it('should return the option if its parent does not exist in options', () => {
        const orphan = options.find((o) => o.value === 'orphan');
        const result = getChainOfParents(orphan, options);
        expect(result).toEqual([orphan]);
    });

    it('should stop chain when a parent in the hierarchy is missing', () => {
        const optionsWithMissingParent: SelectOption[] = [
            { value: 'child', label: 'child', parentValue: 'parent' },
            { value: 'parent', label: 'parent', parentValue: 'missing' },
        ];
        const child = optionsWithMissingParent[0];
        const parent = optionsWithMissingParent[1];
        const result = getChainOfParents(child, optionsWithMissingParent);
        expect(result).toEqual([child, parent]);
    });

    it('should handle deep nesting correctly', () => {
        const deepOptions: SelectOption[] = [
            { value: 'great-grandchild', label: 'great-grandchild', parentValue: 'grandchild' },
            { value: 'grandchild', label: 'grandchild', parentValue: 'child' },
            { value: 'child', label: 'child', parentValue: 'parent' },
            { value: 'parent', label: 'parent' },
        ];
        const greatGrandchild = deepOptions[0];
        const grandchild = deepOptions[1];
        const child = deepOptions[2];
        const parent = deepOptions[3];
        const result = getChainOfParents(greatGrandchild, deepOptions);
        expect(result).toEqual([greatGrandchild, grandchild, child, parent]);
    });
});
