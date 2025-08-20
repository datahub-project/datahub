import { renderHook } from '@testing-library/react-hooks';
import { act } from 'react-dom/test-utils';

import { MenuItemType } from '@components/components/Menu/types';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import useAddPropertyMenuItems from '@app/entityV2/summary/properties/hooks/useAddPropertyMenuItems';

vi.mock('@app/entityV2/summary/properties/context/useAssetPropertiesContext');

const useAssetPropertiesContextMock = vi.mocked(useAssetPropertiesContext);

describe('useAddPropertyMenuItems', () => {
    it('should return sorted menu items from available properties', () => {
        const availableProperties: AssetProperty[] = [
            { name: 'Tags', type: PropertyType.Tags, key: 'tags' },
            { name: 'Owners', type: PropertyType.Owners, key: 'owners' },
        ];
        useAssetPropertiesContextMock.mockReturnValue({ availableProperties } as any);

        const { result } = renderHook(() => useAddPropertyMenuItems(vi.fn()));

        expect(result.current.map((item) => (item as MenuItemType).title)).toEqual(['Owners', 'Tags']);
    });

    it('should call onClick with the correct property when a menu item is clicked', () => {
        const onClick = vi.fn();
        const tagsProperty = { name: 'Tags', type: PropertyType.Tags, key: 'tags' };
        const availableProperties: AssetProperty[] = [
            tagsProperty,
            { name: 'Owners', type: PropertyType.Owners, key: 'owners' },
        ];
        useAssetPropertiesContextMock.mockReturnValue({ availableProperties } as any);

        const { result } = renderHook(() => useAddPropertyMenuItems(onClick));

        act(() => {
            const tagsMenuItem = result.current[1] as MenuItemType;
            tagsMenuItem.onClick?.();
        });

        expect(onClick).toHaveBeenCalledWith(tagsProperty);
    });

    it('should return an empty array when there are no available properties', () => {
        useAssetPropertiesContextMock.mockReturnValue({ availableProperties: [] } as any);

        const { result } = renderHook(() => useAddPropertyMenuItems(vi.fn()));

        expect(result.current).toEqual([]);
    });
});
