import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetAssetsYouOwn } from '@app/homeV2/reference/sections/assets/useGetAssetsYouOwn';
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { CorpUser, EntityType } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleCardsQuery: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: vi.fn(),
}));

vi.mock('@src/app/entityV2/user/useGetUserGroupUrns', () => ({
    default: vi.fn(),
}));

vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    useModuleContext: () => ({ isReloading: false, onReloadingFinished: vi.fn() }),
}));

function mockCorpUser(urn: string): CorpUser {
    const username = urn.includes(':') ? (urn.split(':').pop() as string) : urn;
    return {
        urn,
        type: EntityType.CorpUser,
        username,
    };
}

describe('useGetAssetsYouOwn', () => {
    const queryMock = useGetSearchResultsForMultipleCardsQuery as unknown as Mock;
    const registryMock = useEntityRegistryV2 as unknown as Mock;
    const groupUrnsMock = useGetUserGroupUrns as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        groupUrnsMock.mockReturnValue({ groupUrns: ['urn:li:corpGroup:eng'], loading: false });
        registryMock.mockReturnValue({
            getGenericEntityProperties: vi.fn((_type, entity) => ({ urn: entity.urn })),
        });
        queryMock.mockReturnValue({
            loading: false,
            data: undefined,
            error: undefined,
            refetch: vi.fn(),
        });
    });

    it('uses the trimmed cards query with owner filter and skipCache', () => {
        const user = mockCorpUser('urn:li:corpuser:alice');
        renderHook(() => useGetAssetsYouOwn(user));

        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    input: {
                        query: '*',
                        start: 0,
                        count: 50,
                        types: [],
                        filters: [
                            {
                                field: OWNERS_FILTER_NAME,
                                values: [user.urn, 'urn:li:corpGroup:eng'],
                            },
                        ],
                        searchFlags: { skipCache: true },
                    },
                },
                skip: false,
                fetchPolicy: 'cache-first',
            }),
        );
    });

    it('skips the query when the user has no urn', () => {
        renderHook(() => useGetAssetsYouOwn(undefined));
        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
    });

    it('skips while group urns are loading', () => {
        groupUrnsMock.mockReturnValue({ groupUrns: [], loading: true });
        const user = mockCorpUser('urn:li:corpuser:alice');
        renderHook(() => useGetAssetsYouOwn(user));

        expect(queryMock).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
    });
});
