import { renderHook } from '@testing-library/react-hooks';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
    TAGS_PROPERTY,
    TERMS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';
import { EntityType } from '@types';
import useAvailableAssetProperties from '@app/entityV2/summary/properties/hooks/useAvailableAssetProperties';

vi.mock('@app/entity/shared/EntityContext');

const useEntityContextMock = vi.mocked(useEntityContext);

describe('useAvailableAssetProperties', () => {
    it('should return domain properties', () => {
        useEntityContextMock.mockReturnValue({ entityType: EntityType.Domain } as any);

        const { result } = renderHook(() => useAvailableAssetProperties());

        expect(result.current.availableProperties).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY]);
        expect(result.current.availableStructuredProperties).toEqual([]);
    });

    it('should return glossary term properties', () => {
        useEntityContextMock.mockReturnValue({ entityType: EntityType.GlossaryTerm } as any);

        const { result } = renderHook(() => useAvailableAssetProperties());

        expect(result.current.availableProperties).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY]);
        expect(result.current.availableStructuredProperties).toEqual([]);
    });

    it('should return data product properties', () => {
        useEntityContextMock.mockReturnValue({ entityType: EntityType.DataProduct } as any);

        const { result } = renderHook(() => useAvailableAssetProperties());

        expect(result.current.availableProperties).toEqual([
            CREATED_PROPERTY,
            OWNERS_PROPERTY,
            DOMAIN_PROPERTY,
            TAGS_PROPERTY,
            TERMS_PROPERTY,
        ]);
        expect(result.current.availableStructuredProperties).toEqual([]);
    });

    it('should return an empty array for other entity types', () => {
        useEntityContextMock.mockReturnValue({ entityType: EntityType.Dataset } as any);

        const { result } = renderHook(() => useAvailableAssetProperties());

        expect(result.current.availableProperties).toEqual([]);
        expect(result.current.availableStructuredProperties).toEqual([]);
    });
});
