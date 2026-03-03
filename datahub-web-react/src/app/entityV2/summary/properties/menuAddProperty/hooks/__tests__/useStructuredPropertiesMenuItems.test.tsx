import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MenuItemType } from '@components/components/Menu/types';

import useStructuredPropertiesV2 from '@app/entityV2/summary/properties/hooks/useStructuredProperties';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { PropertyType } from '@app/entityV2/summary/properties/types';

vi.mock('@app/entityV2/summary/properties/hooks/useStructuredProperties');
vi.mock('@components/components/Menu/utils', () => ({
    sortMenuItems: vi.fn((items) => items),
}));
vi.mock('@app/entityV2/summary/properties/menuAddProperty/components/MenuLoader', () => ({
    default: () => <div />,
}));
vi.mock('@app/entityV2/summary/properties/menuAddProperty/components/MenuNoResultsFound', () => ({
    default: () => <div />,
}));
vi.mock('@app/entityV2/summary/properties/menuAddProperty/components/MenuSearchBar', () => ({
    default: () => <div />,
}));

const mockStructuredProperties = [
    { name: 'Prop 1', type: PropertyType.StructuredProperty, key: 'prop1' },
    { name: 'Prop 2', type: PropertyType.StructuredProperty, key: 'prop2' },
];

describe('useStructuredPropertiesMenuItems', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('should return an empty array if there are no structured properties initially', () => {
        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: [],
            loading: false,
        });
        const { result } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));
        expect(result.current).toEqual([]);
    });

    it('should show loader when loading and no properties are available', () => {
        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: [],
            loading: true,
        });
        const { result, rerender } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));

        // Manually trigger the effect that sets hasAnyStructuredProperties
        act(() => {
            (useStructuredPropertiesV2 as any).mockReturnValue({
                structuredProperties: mockStructuredProperties,
                loading: false,
            });
        });
        rerender();

        act(() => {
            (useStructuredPropertiesV2 as any).mockReturnValue({
                structuredProperties: [],
                loading: true,
            });
        });
        rerender();

        expect(result.current.some((item) => item.key === 'loading')).toBe(true);
    });

    it('should show search results', () => {
        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: mockStructuredProperties,
            loading: false,
        });
        const { result } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));
        expect(result.current.length).toBe(3); // search bar + 2 properties
        expect(result.current[1].key).toBe('prop1');
    });

    it('should show no results found message', () => {
        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: mockStructuredProperties,
            loading: false,
        });
        const { result, rerender } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));

        act(() => {
            const searchBar = result.current[0] as MenuItemType;
            const render = searchBar.render as any;
            render().props.onChange('nonexistent');
        });

        act(() => {
            vi.runAllTimers();
        });

        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: [],
            loading: false,
        });
        rerender();

        expect(result.current.some((item) => item.key === 'noResults')).toBe(true);
    });
});
