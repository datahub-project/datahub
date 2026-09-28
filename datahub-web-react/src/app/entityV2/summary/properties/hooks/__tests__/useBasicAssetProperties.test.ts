import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
    TAGS_PROPERTY,
    TERMS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';
import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';

import { EntityType } from '@types';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityContext: vi.fn(),
}));

describe('useBasicAssetProperties', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return correct properties for Domain entity type', () => {
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.Domain });
        const { result } = renderHook(() => useBasicAssetProperties());
        expect(result.current).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY]);
    });

    it('should return correct properties for GlossaryTerm entity type', () => {
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.GlossaryTerm });
        const { result } = renderHook(() => useBasicAssetProperties());
        expect(result.current).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY]);
    });

    it('should return correct properties for GlossaryNode entity type', () => {
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.GlossaryNode });
        const { result } = renderHook(() => useBasicAssetProperties());
        expect(result.current).toEqual([CREATED_PROPERTY, OWNERS_PROPERTY]);
    });

    it.each([
        EntityType.DataProduct,
        EntityType.Dataset,
        EntityType.Application,
        EntityType.Container,
        EntityType.Chart,
        EntityType.Dashboard,
    ])('should return the shared asset property strip for %s', (entityType) => {
        (useEntityContext as any).mockReturnValue({ entityType });
        const { result } = renderHook(() => useBasicAssetProperties());
        expect(result.current).toEqual([
            CREATED_PROPERTY,
            OWNERS_PROPERTY,
            DOMAIN_PROPERTY,
            TAGS_PROPERTY,
            TERMS_PROPERTY,
        ]);
    });

    it('should return an empty array for other entity types', () => {
        (useEntityContext as any).mockReturnValue({ entityType: EntityType.DataFlow });
        const { result } = renderHook(() => useBasicAssetProperties());
        expect(result.current).toEqual([]);
    });
});
