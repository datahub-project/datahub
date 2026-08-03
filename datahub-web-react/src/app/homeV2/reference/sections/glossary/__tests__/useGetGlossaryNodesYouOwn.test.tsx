import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetGlossaryNodesYouOwn } from '@app/homeV2/reference/sections/glossary/useGetGlossaryNodesYouOwn';
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
    useEntityRegistry: vi.fn(),
}));

describe('useGetGlossaryNodesYouOwn', () => {
    const queryMock = useGetSearchResultsForMultipleCardsQuery as unknown as Mock;
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

    it('skips the query when the user is missing', () => {
        renderHook(() => useGetGlossaryNodesYouOwn(undefined));
        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
    });

    it('uses the cards search query for glossary terms owned by the user', () => {
        const user = mockCorpUser('urn:li:corpuser:alice');
        renderHook(() => useGetGlossaryNodesYouOwn(user));

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    query: '*',
                    start: 0,
                    count: 50,
                    types: [EntityType.GlossaryTerm],
                    filters: [{ field: OWNERS_FILTER_NAME, values: [user.urn] }],
                    searchFlags: { skipCache: true },
                },
            },
            skip: false,
            fetchPolicy: 'cache-first',
        });
    });

    it('maps search results through the entity registry', () => {
        const user = mockCorpUser('urn:li:corpuser:carol');
        const getProps = vi.fn((_type, entity) => ({ urn: entity.urn, mapped: true }));
        registryMock.mockReturnValue({ getGenericEntityProperties: getProps });

        queryMock.mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [{ entity: { urn: 'urn:li:glossaryTerm:1', type: EntityType.GlossaryTerm } }],
                },
            },
            error: undefined,
        });

        const { result } = renderHook(() => useGetGlossaryNodesYouOwn(user));

        expect(getProps).toHaveBeenCalled();
        expect(result.current.entities).toEqual([{ urn: 'urn:li:glossaryTerm:1', mapped: true }]);
    });
});
