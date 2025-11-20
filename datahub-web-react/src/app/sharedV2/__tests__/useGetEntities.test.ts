import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { type Entity, EntityType } from '@types';

// Mocking the query hook
vi.mock('@graphql/entity.generated', () => ({
    useGetEntitiesQuery: vi.fn(),
}));

const useGetEntitiesQueryMock = useGetEntitiesQuery as unknown as ReturnType<typeof vi.fn>;

const VALID_URNS = ['urn:li:dataset:123', 'urn:li:chart:456'];
const MIXED_URNS = ['notAUrn', 'urn:li:dataset:1', 'foo', 'urn:li:chart:2'];
const INVALID_URNS = ['foo', 'bar', 'notUrn'];

const MOCK_ENTITIES: Entity[] = [
    { urn: 'urn:li:dataset:123', type: EntityType.Dataset },
    { urn: 'urn:li:chart:456', type: EntityType.Chart },
];
const PARTIAL_ENTITIES: Entity[] = [{ urn: 'urn:li:chart:2', type: EntityType.Chart }];

describe('useGetEntities', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should filter for valid urns and call useGetEntitiesQuery', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: { entities: MOCK_ENTITIES }, loading: false });
        renderHook(() => useGetEntities([...VALID_URNS]));
        expect(useGetEntitiesQueryMock).toHaveBeenCalledWith({
            variables: { urns: VALID_URNS },
            skip: false,
            fetchPolicy: 'cache-first',
        });
    });

    it('should filter out invalid urns', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: { entities: PARTIAL_ENTITIES }, loading: false });
        renderHook(() => useGetEntities([...MIXED_URNS]));
        expect(useGetEntitiesQueryMock).toHaveBeenCalledWith({
            variables: { urns: ['urn:li:dataset:1', 'urn:li:chart:2'] },
            skip: false,
            fetchPolicy: 'cache-first',
        });
    });

    it('should skip query if no valid urns are provided', () => {
        renderHook(() => useGetEntities([...INVALID_URNS]));
        expect(useGetEntitiesQueryMock).toHaveBeenCalledWith({
            variables: { urns: [] },
            skip: true,
            fetchPolicy: 'cache-first',
        });
    });

    it('should skip query if urns array is empty', () => {
        renderHook(() => useGetEntities([]));
        expect(useGetEntitiesQueryMock).toHaveBeenCalledWith({
            variables: { urns: [] },
            skip: true,
            fetchPolicy: 'cache-first',
        });
    });

    it('should return loading=true when query is loading', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: undefined, loading: true });
        const { result } = renderHook(() => useGetEntities([...VALID_URNS]));
        expect(result.current.loading).toBe(true);
        expect(result.current.entities).toEqual([]);
    });

    it('should return entities from data', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: { entities: MOCK_ENTITIES }, loading: false });
        const { result } = renderHook(() => useGetEntities([...VALID_URNS]));
        expect(result.current.entities).toEqual(MOCK_ENTITIES);
        expect(result.current.loading).toBe(false);
    });

    it('should return an empty array of entities if data is undefined', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: undefined, loading: false });
        const { result } = renderHook(() => useGetEntities([...VALID_URNS]));
        expect(result.current.entities).toEqual([]);
    });

    it('should return an empty array if data.entities is missing', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: {}, loading: false });
        const { result } = renderHook(() => useGetEntities([...VALID_URNS]));
        expect(result.current.entities).toEqual([]);
    });

    it('should return an empty array if data.entities is null', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: { entities: null }, loading: false });
        const { result } = renderHook(() => useGetEntities([...VALID_URNS]));
        expect(result.current.entities).toEqual([]);
    });

    it('should default to empty array if data.entities is not an array', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: { entities: 123 }, loading: false });
        const { result } = renderHook(() => useGetEntities([...VALID_URNS]));
        expect(Array.isArray(result.current.entities)).toBe(true);
        expect(result.current.entities.length).toBe(0);
    });

    it('should call with skip: true if all urns are filtered out (not matching urn:li:)', () => {
        renderHook(() => useGetEntities(['foo', 'bar']));
        expect(useGetEntitiesQueryMock).toHaveBeenCalledWith({
            variables: { urns: [] },
            skip: true,
            fetchPolicy: 'cache-first',
        });
    });

    it('should handle mixed scenarios: valid, invalid, undefined', () => {
        useGetEntitiesQueryMock.mockReturnValue({ data: { entities: PARTIAL_ENTITIES }, loading: false });
        renderHook(() => useGetEntities(['urn:li:chart:2', undefined as any, 'bad', null as any]));
        expect(useGetEntitiesQueryMock).toHaveBeenCalledWith({
            variables: { urns: ['urn:li:chart:2'] },
            skip: false,
            fetchPolicy: 'cache-first',
        });
    });
});
