import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { MenuItemType } from '@components/components/Menu/types';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import usePropertyMenuItems from '@app/entityV2/summary/properties/menuProperty/usePropertyMenuItems';
import { PropertyType } from '@app/entityV2/summary/properties/types';

vi.mock('@app/entityV2/summary/properties/context/useAssetPropertiesContext');
vi.mock('@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems');

const mockRemove = vi.fn();
const mockReplace = vi.fn();
const mockAddPropertyMenuItems = [{ type: 'item', key: 'add1', title: 'Add Item 1' }];

describe('usePropertyMenuItems', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (useAssetPropertiesContext as any).mockReturnValue({
            remove: mockRemove,
            replace: mockReplace,
        });
        (useAddPropertyMenuItems as any).mockReturnValue(mockAddPropertyMenuItems);
    });

    it('should return the correct menu structure', () => {
        const { result } = renderHook(() => usePropertyMenuItems(0));
        expect(result.current).toHaveLength(2);
        const replaceItem = result.current[0] as MenuItemType;
        expect(replaceItem.key).toBe('replace');
        expect(replaceItem.children).toEqual(mockAddPropertyMenuItems);
        const removeItem = result.current[1] as MenuItemType;
        expect(removeItem.key).toBe('remove');
    });

    it('should call remove with the correct position when remove is clicked', () => {
        const { result } = renderHook(() => usePropertyMenuItems(5));
        const removeItem = result.current[1] as MenuItemType;
        act(() => {
            removeItem.onClick!();
        });
        expect(mockRemove).toHaveBeenCalledWith(5);
    });

    it('should call replace with the correct arguments when onReplace is called', () => {
        renderHook(() => usePropertyMenuItems(3));
        const onReplace = (useAddPropertyMenuItems as any).mock.calls[0][0];
        const newProperty = { name: 'new', type: PropertyType.Domain };
        act(() => {
            onReplace(newProperty);
        });
        expect(mockReplace).toHaveBeenCalledWith(newProperty, 3);
    });
});
