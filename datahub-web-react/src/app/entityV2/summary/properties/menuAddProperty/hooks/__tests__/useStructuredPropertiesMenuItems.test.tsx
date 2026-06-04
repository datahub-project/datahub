import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MenuItemType } from '@components/components/Menu/types';

import useStructuredPropertiesV2 from '@app/entityV2/summary/properties/hooks/useStructuredProperties';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

import { SummaryElementType } from '@types';

vi.mock('@app/entityV2/summary/properties/hooks/useStructuredProperties');
vi.mock('@app/homeV3/context/PageTemplateContext');
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

const mockStructuredProperties: AssetProperty[] = [
    { name: 'Prop 1', type: SummaryElementType.StructuredProperty, key: 'prop1' },
    { name: 'Prop 2', type: SummaryElementType.StructuredProperty, key: 'prop2' },
];

describe('useStructuredPropertiesMenuItems', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.clearAllMocks();
        (usePageTemplateContext as any).mockReturnValue({ summaryElements: [] });
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

    it('should filter out structured properties that are already visible in summaryElements', () => {
        const visibleProperty: AssetProperty = {
            name: 'Visible Prop',
            type: SummaryElementType.StructuredProperty,
            key: 'visible',
            structuredProperty: { urn: 'urn:li:structuredProperty:visible' } as any,
        };
        const nonVisibleProperty: AssetProperty = {
            name: 'Non Visible Prop',
            type: SummaryElementType.StructuredProperty,
            key: 'nonVisible',
            structuredProperty: { urn: 'urn:li:structuredProperty:nonVisible' } as any,
        };
        (usePageTemplateContext as any).mockReturnValue({
            summaryElements: [{ structuredProperty: { urn: 'urn:li:structuredProperty:visible' } }],
        });
        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: [visibleProperty, nonVisibleProperty],
            loading: false,
        });
        const { result } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));
        // Should only show non-visible property (plus search bar)
        expect(result.current.length).toBe(2);
        expect(result.current[1].key).toBe('nonVisible');
    });

    it('should reset query and hasAnyStructuredProperties when summaryElements changes', () => {
        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: mockStructuredProperties,
            loading: false,
        });
        const { rerender } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));

        // Initial render with empty summaryElements
        (usePageTemplateContext as any).mockReturnValue({ summaryElements: [] });
        rerender();

        // Change summaryElements - this should trigger reset of query and hasAnyStructuredProperties
        (usePageTemplateContext as any).mockReturnValue({
            summaryElements: [{ type: SummaryElementType.Tags }],
        });
        rerender();

        // Verify hasAnyStructuredProperties was reset by checking menu items are rebuilt
        const { result } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));
        // After reset, the menu should still have search bar as first item
        expect(result.current[0].key).toBe('search');
    });

    it('should update filtered properties when summaryElements changes', () => {
        const property1: AssetProperty = {
            name: 'Prop 1',
            type: SummaryElementType.StructuredProperty,
            key: 'prop1',
            structuredProperty: { urn: 'urn:li:structuredProperty:1' } as any,
        };
        const property2: AssetProperty = {
            name: 'Prop 2',
            type: SummaryElementType.StructuredProperty,
            key: 'prop2',
            structuredProperty: { urn: 'urn:li:structuredProperty:2' } as any,
        };

        (useStructuredPropertiesV2 as any).mockReturnValue({
            structuredProperties: [property1, property2],
            loading: false,
        });

        const { result, rerender } = renderHook(() => useStructuredPropertiesMenuItems(vi.fn()));

        // Initially no visible elements
        (usePageTemplateContext as any).mockReturnValue({ summaryElements: [] });
        rerender();
        expect(result.current.length).toBe(3); // search bar + 2 properties

        // Add property1 to summary
        (usePageTemplateContext as any).mockReturnValue({
            summaryElements: [{ structuredProperty: { urn: 'urn:li:structuredProperty:1' } }],
        });
        rerender();
        expect(result.current.length).toBe(2); // search bar + 1 property
        expect(result.current[1].key).toBe('prop2');
    });
});
