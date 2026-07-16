import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { CREATED_PROPERTY, DOMAIN_PROPERTY, OWNERS_PROPERTY } from '@app/entityV2/summary/properties/constants';
import useInitialAssetProperties from '@app/entityV2/summary/properties/hooks/useInitialAssetProperties';
import useAssetProperties from '@app/entityV2/summary/properties/hooks/usePropertiesFromAsset';
import { PropertyType } from '@app/entityV2/summary/properties/types';

import { EntityType } from '@types';

vi.mock('@app/entityV2/summary/properties/hooks/usePropertiesFromAsset');

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityContext: vi.fn(),
}));

describe('useInitialAssetProperties', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return entity asset properties when available', () => {
        const entityAssetProperties = [{ name: 'test', type: PropertyType.Domain }];
        (useAssetProperties as any).mockReturnValue({
            assetProperties: entityAssetProperties,
            loading: false,
        });
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.Dataset });

        const { result } = renderHook(() => useInitialAssetProperties('test-urn'));

        expect(result.current.properties).toEqual(entityAssetProperties);
        expect(result.current.loading).toBe(false);
    });

    it('should return default properties when entity asset properties are not available', () => {
        (useAssetProperties as any).mockReturnValue({
            assetProperties: null,
            loading: false,
        });
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.GlossaryTerm });

        const { result } = renderHook(() => useInitialAssetProperties('test-urn'));

        expect(result.current.properties).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY]);
        expect(result.current.loading).toBe(false);
    });

    it('should return loading state from useAssetProperties', () => {
        (useAssetProperties as any).mockReturnValue({
            assetProperties: null,
            loading: true,
        });
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.Dataset });

        const { result } = renderHook(() => useInitialAssetProperties('test-urn'));

        expect(result.current.loading).toBe(true);
    });
});
