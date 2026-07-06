import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetDomainsYouOwn } from '@app/homeV2/reference/sections/domains/useGetDomainsYouOwn';
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
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
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(),
}));

describe('useGetDomainsYouOwn', () => {
    const queryMock = useGetSearchResultsForMultipleQuery as unknown as Mock;
    const registryMock = useEntityRegistry as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        registryMock.mockReturnValue({
            getGenericEntityProperties: vi.fn((_type, entity) => ({ urn: entity.urn })),
        });
        queryMock.mockReturnValue({
            loading: false,
            data: undefined,
            error: undefined,
        });
    });

    it('skips the query when the user has no urn', () => {
        renderHook(() => useGetDomainsYouOwn(undefined));
        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
    });

    it('searches domains owned by the user using values-only owner filter', () => {
        const user = mockCorpUser('urn:li:corpuser:alice');
        renderHook(() => useGetDomainsYouOwn(user));

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    query: '*',
                    start: 0,
                    count: 50,
                    types: [EntityType.Domain],
                    searchFlags: { skipCache: true },
                    filters: [{ field: OWNERS_FILTER_NAME, values: [user.urn] }],
                },
            },
            skip: false,
            fetchPolicy: 'cache-first',
        });
    });

    it('respects custom count', () => {
        const user = mockCorpUser('urn:li:corpuser:bob');
        renderHook(() => useGetDomainsYouOwn(user, 12));

        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ count: 12 }),
                }),
            }),
        );
    });

    it('maps search results through the entity registry', () => {
        const user = mockCorpUser('urn:li:corpuser:carol');
        const getProps = vi.fn((_type, entity) => ({ urn: entity.urn, mapped: true }));
        registryMock.mockReturnValue({ getGenericEntityProperties: getProps });

        queryMock.mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [{ entity: { urn: 'urn:li:domain:1', type: EntityType.Domain } }],
                },
            },
            error: undefined,
        });

        const { result } = renderHook(() => useGetDomainsYouOwn(user));

        expect(getProps).toHaveBeenCalled();
        expect(result.current.entities).toEqual([{ urn: 'urn:li:domain:1', mapped: true }]);
    });
});
