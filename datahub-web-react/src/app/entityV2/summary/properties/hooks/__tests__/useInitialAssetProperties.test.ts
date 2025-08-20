import { renderHook } from '@testing-library/react-hooks';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
    TAGS_PROPERTY,
    TERMS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';
import useInitialAssetProperties from '@app/entityV2/summary/properties/hooks/useInitialAssetProperties';
import useAssetProperties from '@app/entityV2/summary/properties/hooks/usePropertiesFromAsset';
import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import { EntityType } from '@types';

vi.mock('@app/entityV2/summary/properties/hooks/usePropertiesFromAsset');

const useAssetPropertiesMock = vi.mocked(useAssetProperties);

describe('useInitialAssetProperties', () => {
    it('should return default properties for Domain entity type', () => {
        useAssetPropertiesMock.mockReturnValue({ assetProperties: null, loading: false } as any);

        const { result } = renderHook(() => useInitialAssetProperties('urn:li:domain:1', EntityType.Domain));

        expect(result.current.properties).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY]);
        expect(result.current.loading).toBe(false);
    });

    it('should return default properties for GlossaryTerm entity type', () => {
        useAssetPropertiesMock.mockReturnValue({ assetProperties: null, loading: false } as any);

        const { result } = renderHook(() => useInitialAssetProperties('urn:li:glossaryTerm:1', EntityType.GlossaryTerm));

        expect(result.current.properties).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY]);
        expect(result.current.loading).toBe(false);
    });

    it('should return default properties for GlossaryNode entity type', () => {
        useAssetPropertiesMock.mockReturnValue({ assetProperties: null, loading: false } as any);

        const { result } = renderHook(() => useInitialAssetProperties('urn:li:glossaryNode:1', EntityType.GlossaryNode));

        expect(result.current.properties).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY]);
        expect(result.current.loading).toBe(false);
    });

    it('should return default properties for DataProduct entity type', () => {
        useAssetPropertiesMock.mockReturnValue({ assetProperties: null, loading: false } as any);

        const { result } = renderHook(() => useInitialAssetProperties('urn:li:dataProduct:1', EntityType.DataProduct));

        expect(result.current.properties).toEqual([
            CREATED_PROPERTY,
            OWNERS_PROPERTY,
            DOMAIN_PROPERTY,
            TAGS_PROPERTY,
            TERMS_PROPERTY,
        ]);
        expect(result.current.loading).toBe(false);
    });

    it('should return entity asset properties when available', () => {
        const entityAssetProperties: AssetProperty[] = [
            {
                key: 'owners',
                name: 'Owners',
                type: PropertyType.Owners,
            },
        ];
        useAssetPropertiesMock.mockReturnValue({ assetProperties: entityAssetProperties, loading: false });

        const { result } = renderHook(() => useInitialAssetProperties('urn:li:dataset:1', EntityType.Dataset));

        expect(result.current.properties).toEqual(entityAssetProperties);
        expect(result.current.loading).toBe(false);
    });

    it('should return loading state', () => {
        useAssetPropertiesMock.mockReturnValue({ assetProperties: null, loading: true } as any);

        const { result } = renderHook(() => useInitialAssetProperties('urn:li:dataset:1', EntityType.Dataset));

        expect(result.current.loading).toBe(true);
    });
});
