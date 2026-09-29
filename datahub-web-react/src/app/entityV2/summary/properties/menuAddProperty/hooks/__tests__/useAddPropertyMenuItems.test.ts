import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { MenuItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';
import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

import { SummaryElementType } from '@types';

vi.mock('@app/entityV2/summary/properties/hooks/useBasicAssetProperties');
vi.mock('@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems');
vi.mock('@app/homeV3/context/PageTemplateContext');
vi.mock('@components/components/Menu/utils', () => ({
    sortMenuItems: vi.fn((items) => items),
}));

const mockBasicProperties: AssetProperty[] = [
    { name: 'Tags', type: SummaryElementType.Tags },
    { name: 'Owners', type: SummaryElementType.Owners },
];

const mockStructuredPropertiesMenuItems = [{ type: 'item', key: 'structured1', title: 'Structured Prop 1' }];

describe('useAddPropertyMenuItems', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (usePageTemplateContext as any).mockReturnValue({ summaryElements: [] });
    });

    it('should return only basic properties when no structured properties are available', () => {
        (useBasicAssetProperties as any).mockReturnValue(mockBasicProperties);
        (useStructuredPropertiesMenuItems as any).mockReturnValue([]);
        const { result } = renderHook(() => useAddPropertyMenuItems(vi.fn()));
        expect(result.current).toHaveLength(2);
        expect((result.current[0] as MenuItemType).title).toBe('Tags');
    });

    it('should combine basic and structured properties', () => {
        (useBasicAssetProperties as any).mockReturnValue(mockBasicProperties);
        (useStructuredPropertiesMenuItems as any).mockReturnValue(mockStructuredPropertiesMenuItems);
        const { result } = renderHook(() => useAddPropertyMenuItems(vi.fn()));
        expect(result.current).toHaveLength(3);
        const structuredPropertyGroup = result.current[2] as MenuItemType;
        expect(structuredPropertyGroup.title).toBe('Properties');
        expect(structuredPropertyGroup.children).toEqual(mockStructuredPropertiesMenuItems);
    });

    it('should call onClick when a basic property is clicked', () => {
        const onClick = vi.fn();
        (useBasicAssetProperties as any).mockReturnValue(mockBasicProperties);
        (useStructuredPropertiesMenuItems as any).mockReturnValue([]);
        const { result } = renderHook(() => useAddPropertyMenuItems(onClick));
        act(() => {
            (result.current[0] as MenuItemType).onClick!();
        });
        expect(onClick).toHaveBeenCalledWith(mockBasicProperties[0]);
    });

    it('should call sortMenuItems', () => {
        (useBasicAssetProperties as any).mockReturnValue(mockBasicProperties);
        (useStructuredPropertiesMenuItems as any).mockReturnValue([]);
        renderHook(() => useAddPropertyMenuItems(vi.fn()));
        expect(sortMenuItems).toHaveBeenCalled();
    });

    it('should filter out basic properties that are already visible in summaryElements', () => {
        (usePageTemplateContext as any).mockReturnValue({
            summaryElements: [{ type: SummaryElementType.Tags }],
        });
        (useBasicAssetProperties as any).mockReturnValue(mockBasicProperties);
        (useStructuredPropertiesMenuItems as any).mockReturnValue([]);
        const { result } = renderHook(() => useAddPropertyMenuItems(vi.fn()));
        expect(result.current).toHaveLength(1);
        expect((result.current[0] as MenuItemType).title).toBe('Owners');
    });

    it('should filter out structured properties that are already visible in summaryElements', () => {
        const visibleStructuredProperty: AssetProperty = {
            name: 'Visible Prop',
            type: SummaryElementType.StructuredProperty,
            key: 'visible',
            structuredProperty: { urn: 'urn:li:structuredProperty:visible' } as any,
        };
        const nonVisibleStructuredProperty: AssetProperty = {
            name: 'Non Visible Prop',
            type: SummaryElementType.StructuredProperty,
            key: 'nonVisible',
            structuredProperty: { urn: 'urn:li:structuredProperty:nonVisible' } as any,
        };
        (usePageTemplateContext as any).mockReturnValue({
            summaryElements: [{ structuredProperty: { urn: 'urn:li:structuredProperty:visible' } }],
        });
        (useBasicAssetProperties as any).mockReturnValue([visibleStructuredProperty, nonVisibleStructuredProperty]);
        (useStructuredPropertiesMenuItems as any).mockReturnValue([]);
        const { result } = renderHook(() => useAddPropertyMenuItems(vi.fn()));
        expect(result.current).toHaveLength(1);
        expect((result.current[0] as MenuItemType).title).toBe('Non Visible Prop');
    });

    it('should update menu items when summaryElements changes', () => {
        const { result, rerender } = renderHook(() => useAddPropertyMenuItems(vi.fn()), {
            initialProps: { summaryElements: [] },
        });
        (useBasicAssetProperties as any).mockReturnValue(mockBasicProperties);
        (useStructuredPropertiesMenuItems as any).mockReturnValue([]);

        // Initial render with no visible elements
        (usePageTemplateContext as any).mockReturnValue({ summaryElements: [] });
        rerender();
        expect(result.current).toHaveLength(2);

        // After adding Tags to summary
        (usePageTemplateContext as any).mockReturnValue({
            summaryElements: [{ type: SummaryElementType.Tags }],
        });
        rerender();
        expect(result.current).toHaveLength(1);
    });
});
