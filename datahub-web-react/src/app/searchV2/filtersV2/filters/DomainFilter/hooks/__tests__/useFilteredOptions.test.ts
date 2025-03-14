import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { renderHook } from '@testing-library/react-hooks';
import { EntityType } from '@src/types.generated';
import useFilteredOptions from '../useFilteredOptions';

describe('useFilteredOptions', () => {
    it('should filter options', () => {
        const option = {
            value: 'option',
            label: 'option',
            entity: { urn: 'option', type: EntityType.Domain },
        };
        const anotherOption = {
            value: 'another',
            label: 'another',
            entity: { urn: 'another', type: EntityType.Domain },
        };
        const options: SelectOption[] = [option, anotherOption];

        const result = renderHook(() => useFilteredOptions(options, 'option')).result.current;

        expect(result).toStrictEqual([option]);
    });

    it('should keep parent options', () => {
        const anotherChild = {
            value: 'another',
            label: 'another',
            entity: { urn: 'another', type: EntityType.Domain },
            parentValue: 'parent',
        };
        const child = {
            value: 'child',
            label: 'child',
            entity: { urn: 'child', type: EntityType.Domain },
            parentValue: 'parent',
        };
        const parent = {
            value: 'parent',
            entity: { urn: 'parent', type: EntityType.Domain },
            label: 'parent',
        };
        const options: SelectOption[] = [anotherChild, child, parent];

        const result = renderHook(() => useFilteredOptions(options, 'child')).result.current;

        expect(result).toStrictEqual([child, parent]);
    });

    it('should return the same options when query is empty', () => {
        const option = {
            value: 'option',
            label: 'option',
            entity: { urn: 'option', type: EntityType.Domain },
        };
        const anotherOption = {
            value: 'another',
            label: 'another',
            entity: { urn: 'another', type: EntityType.Domain },
        };
        const options: SelectOption[] = [option, anotherOption];

        const result = renderHook(() => useFilteredOptions(options, '')).result.current;

        expect(result).toStrictEqual(options);
    });
});
