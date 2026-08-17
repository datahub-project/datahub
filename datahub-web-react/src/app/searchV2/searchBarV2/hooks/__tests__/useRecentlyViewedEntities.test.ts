import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import useRecentlyViewedEntities from '@app/searchV2/searchBarV2/hooks/useRecentlyViewedEntities';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { EntityType, RecommendationModuleId, ScenarioType } from '@types';

vi.mock('@graphql/recommendations.generated', () => ({
    useListRecommendationsQuery: vi.fn(),
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

describe('useRecentlyViewedEntities', () => {
    const queryMock = useListRecommendationsQuery as unknown as Mock;
    const userMock = useUserContext as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        userMock.mockReturnValue({
            user: { urn: 'urn:li:corpuser:alice' },
            localState: { selectedViewUrn: undefined },
        });
        queryMock.mockReturnValue({
            data: undefined,
            loading: false,
            refetch: vi.fn(),
        });
    });

    it('requests only recently viewed entities on a dedicated HOME query', () => {
        renderHook(() => useRecentlyViewedEntities());

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    userUrn: 'urn:li:corpuser:alice',
                    requestContext: {
                        scenario: ScenarioType.Home,
                        modules: [RecommendationModuleId.RecentlyViewedEntities],
                    },
                    limit: 1,
                    viewUrn: undefined,
                },
            },
            fetchPolicy: 'cache-first',
            skip: false,
        });
    });

    it('maps recently viewed entities from the module payload', () => {
        const entity = { urn: 'urn:li:dataset:1', type: EntityType.Dataset };
        queryMock.mockReturnValue({
            data: {
                listRecommendations: {
                    modules: [
                        {
                            moduleId: 'RecentlyViewedEntities',
                            content: [{ entity }],
                        },
                    ],
                },
            },
            loading: false,
            refetch: vi.fn(),
        });

        const { result } = renderHook(() => useRecentlyViewedEntities());

        expect(result.current.entities).toEqual([entity]);
    });
});
