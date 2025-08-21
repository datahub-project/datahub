import { renderHook } from '@testing-library/react-hooks';
import { describe, it, expect, vi } from 'vitest';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';
import { PropertyType } from '@app/entityV2/summary/properties/types';
import { EntityType } from '@types';
import useAssetProperties from '@app/entityV2/summary/properties/hooks/usePropertiesFromAsset';
import useInitialAssetProperties from '@app/entityV2/summary/properties/hooks/useInitialAssetProperties';

vi.mock('@app/entityV2/summary/properties/hooks/usePropertiesFromAsset');

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

        const { result } = renderHook(() =>
            useInitialAssetProperties('test-urn', EntityType.Dataset),
        );

        expect(result.current.properties).toEqual(entityAssetProperties);
        expect(result.current.loading).toBe(false);
    });

    it('should return default properties when entity asset properties are not available', () => {
        (useAssetProperties as any).mockReturnValue({
            assetProperties: null,
            loading: false,
        });

        const { result } = renderHook(() =>
            useInitialAssetProperties('test-urn', EntityType.GlossaryTerm),
        );

        expect(result.current.properties).toEqual([
            CREATED_PROPERTY,
            OWNERS_PROPERTY,
            DOMAIN_PROPERTY,
        ]);
        expect(result.current.loading).toBe(false);
    });

    it('should return loading state from useAssetProperties', () => {
        (useAssetProperties as any).mockReturnValue({
            assetProperties: null,
            loading: true,
        });

        const { result } = renderHook(() =>
            useInitialAssetProperties('test-urn', EntityType.Dataset),
        );

        expect(result.current.loading).toBe(true);
    });
});
