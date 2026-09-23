import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useHydratedEntityMap } from '@app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import { useGetEntities } from '@src/app/sharedV2/useGetEntities';

import { type Entity, EntityType } from '@types';

vi.mock('@src/app/sharedV2/useGetEntities', () => ({
    useGetEntities: vi.fn(),
}));

const useGetEntitiesMock = useGetEntities as unknown as ReturnType<typeof vi.fn>;

const TERM: Entity = { urn: 'urn:li:glossaryTerm:a', type: EntityType.GlossaryTerm };
const DATASET: Entity = { urn: 'urn:li:dataset:b', type: EntityType.Dataset };

describe('useHydratedEntityMap', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        useGetEntitiesMock.mockReturnValue({ entities: [TERM, DATASET], loading: false });
    });

    it('dedupes and drops empty urns, and never asks for lineage or siblings', () => {
        renderHook(() => useHydratedEntityMap([TERM.urn, undefined, DATASET.urn, null, TERM.urn]));
        expect(useGetEntitiesMock).toHaveBeenCalledWith([TERM.urn, DATASET.urn], undefined, {
            skipLineage: true,
            skipSiblingsSearch: true,
        });
    });

    it('keys the hydrated entities by urn', () => {
        const { result } = renderHook(() => useHydratedEntityMap([TERM.urn, DATASET.urn]));
        expect(result.current).toEqual({ [TERM.urn]: TERM, [DATASET.urn]: DATASET });
    });

    it('handles no urns at all', () => {
        useGetEntitiesMock.mockReturnValue({ entities: [], loading: false });
        const { result } = renderHook(() => useHydratedEntityMap(undefined));
        expect(useGetEntitiesMock).toHaveBeenCalledWith([], undefined, { skipLineage: true, skipSiblingsSearch: true });
        expect(result.current).toEqual({});
    });
});
