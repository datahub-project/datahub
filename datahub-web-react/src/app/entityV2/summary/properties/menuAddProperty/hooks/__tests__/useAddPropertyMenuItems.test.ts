import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { MenuItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';
import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { PropertyType } from '@app/entityV2/summary/properties/types';

vi.mock('@app/entityV2/summary/properties/hooks/useBasicAssetProperties');
vi.mock('@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems');
vi.mock('@components/components/Menu/utils', () => ({
    sortMenuItems: vi.fn((items) => items),
}));

const mockBasicProperties = [
    { name: 'Tags', type: PropertyType.Tags },
    { name: 'Owners', type: PropertyType.Owners },
];

const mockStructuredPropertiesMenuItems = [{ type: 'item', key: 'structured1', title: 'Structured Prop 1' }];

describe('useAddPropertyMenuItems', () => {
    beforeEach(() => {
        vi.clearAllMocks();
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
});
