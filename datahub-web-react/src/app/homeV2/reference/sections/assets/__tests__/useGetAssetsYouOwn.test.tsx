import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetAssetsYouOwn } from '@app/homeV2/reference/sections/assets/useGetAssetsYouOwn';
import { ASSET_ENTITY_TYPES, OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { CorpUser, EntityType } from '@types';

function mockCorpUser(urn: string): CorpUser {
    const username = urn.includes(':') ? (urn.split(':').pop() as string) : urn;
    return {
        urn,
        type: EntityType.CorpUser,
        username,
    };
}

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleCardsQuery: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: vi.fn(),
}));

vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    useModuleContext: () => ({
        isReloading: false,
        onReloadingFinished: vi.fn(),
    }),
}));

vi.mock('@src/app/entityV2/user/useGetUserGroupUrns', () => ({
    default: () => ({
        groupUrns: [],
        loading: false,
    }),
}));

describe('useGetAssetsYouOwn', () => {
    const queryMock = useGetSearchResultsForMultipleCardsQuery as unknown as Mock;
    const registryMock = useEntityRegistryV2 as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        registryMock.mockReturnValue({
            getGenericEntityProperties: vi.fn((_type, entity) => ({ urn: entity.urn })),
        });
        queryMock.mockReturnValue({
            loading: false,
            data: { searchAcrossEntities: { searchResults: [], total: 0 } },
            error: undefined,
            refetch: vi.fn(),
        });
    });

    it('skips the query when the user has no urn', () => {
        renderHook(() => useGetAssetsYouOwn(undefined));
        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
    });

    it('uses the cards search query for assets owned by the user', () => {
        const user = mockCorpUser('urn:li:corpuser:alice');
        renderHook(() => useGetAssetsYouOwn(user));

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    query: '*',
                    start: 0,
                    count: 50,
                    types: ASSET_ENTITY_TYPES,
                    filters: [{ field: OWNERS_FILTER_NAME, values: [user.urn] }],
                    searchFlags: { skipCache: true },
                },
            },
            skip: false,
            fetchPolicy: 'cache-first',
            onCompleted: expect.any(Function),
        });
    });
});
