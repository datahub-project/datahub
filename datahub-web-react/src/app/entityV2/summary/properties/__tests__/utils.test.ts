import { afterEach, describe, expect, it, vi } from 'vitest';

import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import { assetPropertyToMenuItem } from '@app/entityV2/summary/properties/utils';

describe('assetPropertyToMenuItem', () => {
    const mockOnMenuItemClick = vi.fn();

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should convert an AssetProperty to a MenuItemType with key', () => {
        const assetProperty: AssetProperty = {
            key: 'testKey',
            type: PropertyType.Domain,
            name: 'Test Name',
            icon: 'testIcon',
        };

        const menuItem = assetPropertyToMenuItem(assetProperty, mockOnMenuItemClick);

        expect(menuItem.key).toBe('testKey');
        expect(menuItem.title).toBe('Test Name');
        expect(menuItem.icon).toBe('testIcon');
        expect(menuItem.type).toBe('item');
    });

    it('should use type as key if key is not provided', () => {
        const assetProperty: AssetProperty = {
            type: PropertyType.Domain,
            name: 'Test Name',
            icon: 'testIcon',
        };

        const menuItem = assetPropertyToMenuItem(assetProperty, mockOnMenuItemClick);

        expect(menuItem.key).toBe(PropertyType.Domain);
    });

    it('should call onMenuItemClick with the assetProperty when onClick is triggered', () => {
        const assetProperty: AssetProperty = {
            key: 'testKey',
            type: PropertyType.Domain,
            name: 'Test Name',
            icon: 'testIcon',
        };

        const menuItem = assetPropertyToMenuItem(assetProperty, mockOnMenuItemClick);
        menuItem.onClick?.();

        expect(mockOnMenuItemClick).toHaveBeenCalledWith(assetProperty);
    });
});
