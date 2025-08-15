import { NestedSelectOption } from '@components/components/Select/Nested/types';
import { filterNestedSelectOptions } from '@components/components/Select/Nested/utils';

describe('filterNestedSelectOptions', () => {
    const options: NestedSelectOption[] = [
        { value: 'p1', label: 'Parent 1', isParent: true },
        { value: 'c1', label: 'Child 1', parentValue: 'p1' },
        { value: 'c2', label: 'Child 2', parentValue: 'p1' },
        { value: 'p2', label: 'Parent 2', isParent: true },
        { value: 'c3', label: 'Child 3', parentValue: 'p2' },
    ];

    it('should return all options when query is empty', () => {
        const result = filterNestedSelectOptions(options, '');
        expect(result).toEqual(options);
    });

    it('should include parent of matched child and keep parent as parent', () => {
        const query = 'Child 1';
        const result = filterNestedSelectOptions(options, query);

        expect(result).toEqual([
            { value: 'p1', label: 'Parent 1', isParent: true },
            { value: 'c1', label: 'Child 1', parentValue: 'p1' },
        ]);
    });

    it('should mark parent as non-parent if no children are present in the result', () => {
        const query = 'Parent 1';
        const result = filterNestedSelectOptions(options, query);

        expect(result).toEqual([{ value: 'p1', label: 'Parent 1', isParent: false }]);
    });

    it('should include multiple levels of parents if needed', () => {
        const optionsWithGrandparent: NestedSelectOption[] = [
            { value: 'gp', label: 'Grandparent', isParent: true },
            { value: 'p1', label: 'Parent 1', parentValue: 'gp', isParent: true },
            { value: 'c1', label: 'Child 1', parentValue: 'p1' },
        ];
        const query = 'Child 1';
        const result = filterNestedSelectOptions(optionsWithGrandparent, query);

        expect(result).toEqual([
            { value: 'gp', label: 'Grandparent', isParent: true },
            { value: 'p1', label: 'Parent 1', parentValue: 'gp', isParent: true },
            { value: 'c1', label: 'Child 1', parentValue: 'p1' },
        ]);
    });

    it('should keep parent as parent when both parent and child are matched', () => {
        const query = '1';
        const result = filterNestedSelectOptions(options, query);

        expect(result).toEqual([
            { value: 'p1', label: 'Parent 1', isParent: true },
            { value: 'c1', label: 'Child 1', parentValue: 'p1' },
        ]);
    });
});
