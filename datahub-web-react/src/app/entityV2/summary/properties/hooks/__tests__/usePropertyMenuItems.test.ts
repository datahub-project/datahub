import { renderHook, act } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import usePropertyMenuItems from '@app/entityV2/summary/properties/hooks/usePropertyMenuItems';
import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import { MenuItemType } from '@components/components/Menu/types';

vi.mock('@app/entityV2/summary/properties/context/useAssetPropertiesContext');

const mockRemove = vi.fn();
const mockReplace = vi.fn();
const mockAvailableProperties: AssetProperty[] = [
    { name: 'Domain', type: PropertyType.Domain, key: 'domain' },
    { name: 'Owners', type: PropertyType.Owners, key: 'owners' },
];

describe('usePropertyMenuItems', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(useAssetPropertiesContext).mockReturnValue({
            remove: mockRemove,
            replace: mockReplace,
            availableProperties: mockAvailableProperties,
        } as any);
    });

    it('should return menu items for replace and remove', () => {
        const { result } = renderHook(() => usePropertyMenuItems(0));

        expect(result.current.length).toBe(2);
        expect(result.current[0].key).toBe('replace');
        expect((result.current[0] as MenuItemType).title).toBe('Replace Property');
        expect(result.current[1].key).toBe('remove');
        expect((result.current[1] as MenuItemType).title).toBe('Remove');
    });

    it('should call remove with the correct position when remove is clicked', () => {
        const position = 2;
        const { result } = renderHook(() => usePropertyMenuItems(position));

        const removeMenuItem = result.current.find((item) => item.key === 'remove') as MenuItemType;
        act(() => {
            removeMenuItem?.onClick?.();
        });

        expect(mockRemove).toHaveBeenCalledWith(position);
    });

    it('should have child menu items for replacing a property', () => {
        const { result } = renderHook(() => usePropertyMenuItems(0));

        const replaceMenuItem = result.current.find((item) => item.key === 'replace') as MenuItemType;
        expect(replaceMenuItem?.children?.length).toBe(mockAvailableProperties.length);
        expect((replaceMenuItem?.children?.[0] as MenuItemType).title).toBe('Domain');
        expect((replaceMenuItem?.children?.[1] as MenuItemType).title).toBe('Owners');
    });

    it('should call replace with the correct property and position when a replace sub-item is clicked', () => {
        const position = 1;
        const { result } = renderHook(() => usePropertyMenuItems(position));

        const replaceMenuItem = result.current.find((item) => item.key === 'replace') as MenuItemType;
        const domainReplaceItem = replaceMenuItem?.children?.find((child) => child.key === 'domain') as MenuItemType;

        act(() => {
            domainReplaceItem?.onClick?.();
        });

        expect(mockReplace).toHaveBeenCalledWith(mockAvailableProperties[0], position);
    });
});
