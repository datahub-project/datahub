import { act, renderHook } from '@testing-library/react-hooks';

import useOptions from '@app/ingestV2/shared/components/filters/hooks/useOptions';

interface TestItem {
    id: string;
    name: string;
}

interface TestOption {
    value: string;
    label: string;
    item: TestItem;
}
const itemToOptionConverter = (item: TestItem): TestOption => ({
    value: item.id,
    label: item.name,
    item,
});

describe('useOptions Hook', () => {
    it('initializes with default options', () => {
        const defaultItem = { id: '1', name: 'Default' };
        const defaultItems = [defaultItem];
        const dynamicItems = [];
        const { result } = renderHook(() => useOptions(defaultItems, dynamicItems, itemToOptionConverter));
        expect(result.current.options).toEqual([
            {
                value: '1',
                label: 'Default',
                item: defaultItem,
            },
        ]);
    });

    it('merges default and dynamic options', () => {
        const defaultItem = { id: '1', name: 'Default' };
        const dynamicItem = { id: '2', name: 'Dynamic' };
        const defaultItems = [defaultItem];
        const dynamicItems = [dynamicItem];
        const { result } = renderHook(() => useOptions(defaultItems, dynamicItems, itemToOptionConverter));
        expect(result.current.options).toHaveLength(2);
        expect(result.current.options).toContainEqual(expect.objectContaining({ value: '1' }));
        expect(result.current.options).toContainEqual(expect.objectContaining({ value: '2' }));
    });

    it('prioritizes dynamic options over defaults with the same value', () => {
        const defaultItem = { id: '1', name: 'Default' };
        const dynamicItem = { id: '1', name: 'Dynamic' };
        const defaultItems = [defaultItem];
        const dynamicItems = [dynamicItem];
        const { result } = renderHook(() => useOptions(defaultItems, dynamicItems, itemToOptionConverter));
        expect(result.current.options).toEqual([
            {
                value: '1',
                label: 'Dynamic',
                item: dynamicItem,
            },
        ]);
    });

    it('updates applied options when values are selected', () => {
        const defaultItem = { id: '1', name: 'Default' };
        const dynamicItem = { id: '2', name: 'Dynamic' };
        const defaultItems = [defaultItem];
        const dynamicItems = [dynamicItem];
        const selectedValue = '2';
        const selectedValues = [selectedValue];

        const { result } = renderHook(() => useOptions(defaultItems, dynamicItems, itemToOptionConverter));

        act(() => {
            result.current.onSelectedValuesChanged(selectedValues);
        });

        expect(result.current.options).toEqual([
            {
                value: selectedValue,
                label: 'Dynamic',
                item: dynamicItem,
            },
        ]);
    });

    it('retains selected default options when no dynamic items', () => {
        const defaultItem = { id: '1', name: 'Default' };
        const defaultItems = [defaultItem];
        const dynamicItems = [];
        const selectedValue = '1';
        const selectedValues = [selectedValue];
        const { result } = renderHook(() => useOptions(defaultItems, dynamicItems, itemToOptionConverter));
        act(() => {
            result.current.onSelectedValuesChanged(selectedValues);
        });
        expect(result.current.options).toEqual([
            {
                value: selectedValue,
                label: 'Default',
                item: defaultItem,
            },
        ]);
    });
});
