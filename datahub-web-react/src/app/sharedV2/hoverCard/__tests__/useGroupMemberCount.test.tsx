import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import useGroupMemberCount from '@app/sharedV2/hoverCard/useGroupMemberCount';

import { GetGroupMemberCountDocument, useGetGroupMemberCountQuery } from '@graphql/group.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

// Passes through to the real query; one test swaps in a stale response to pin the urn check.
vi.mock('@graphql/group.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/group.generated')>();
    return { ...actual, useGetGroupMemberCountQuery: vi.fn(actual.useGetGroupMemberCountQuery) };
});

const GROUP_URN = 'urn:li:corpGroup:data-platform';
const OTHER_GROUP_URN = 'urn:li:corpGroup:analytics';
const USER_URN = 'urn:li:corpuser:jdoe';

function memberCountMock(urn: string, total: number, delay = 0) {
    const result = vi.fn(() => ({
        data: {
            corpGroup: {
                __typename: 'CorpGroup',
                urn,
                type: EntityType.CorpGroup,
                memberCount: { __typename: 'EntityRelationshipsResult', total },
            },
        },
    }));
    const mock: MockedResponse = {
        request: { query: GetGroupMemberCountDocument, variables: { urn } },
        result,
        delay,
    };
    return { mock, result };
}

/** Lets a query that was (wrongly) started reach its mock before asserting it never ran. */
function flushQueries() {
    return new Promise((resolve) => {
        setTimeout(resolve, 0);
    });
}

function renderUseGroupMemberCount(entity: Entity, mocks: MockedResponse[]) {
    return renderHook((props: { entity: Entity }) => useGroupMemberCount(props.entity), {
        initialProps: { entity },
        wrapper: ({ children }) => <MockedProvider mocks={mocks}>{children}</MockedProvider>,
    });
}

describe('useGroupMemberCount', () => {
    it('fetches the count when the group was loaded without one', async () => {
        const { mock } = memberCountMock(GROUP_URN, 7);
        const { result, waitFor } = renderUseGroupMemberCount(
            { urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup,
            [mock],
        );

        expect(result.current).toBeUndefined();
        await waitFor(() => expect(result.current).toBe(7));
    });

    it('uses a count the group was loaded with, even zero, without fetching', async () => {
        const { mock, result: fetch } = memberCountMock(GROUP_URN, 7);
        const { result } = renderUseGroupMemberCount(
            { urn: GROUP_URN, type: EntityType.CorpGroup, memberCount: { total: 0 } } as unknown as CorpGroup,
            [mock],
        );

        expect(result.current).toBe(0);
        await flushQueries();
        expect(fetch).not.toHaveBeenCalled();
    });

    it('fetches the count rather than trust a plain relationships total, which may count one kind of membership', async () => {
        const { mock } = memberCountMock(GROUP_URN, 7);
        const { result, waitFor } = renderUseGroupMemberCount(
            { urn: GROUP_URN, type: EntityType.CorpGroup, relationships: { total: 3 } } as unknown as CorpGroup,
            [mock],
        );

        expect(result.current).toBeUndefined();
        await waitFor(() => expect(result.current).toBe(7));
    });

    it('returns nothing for entities other than groups, without fetching', async () => {
        const { mock, result: fetch } = memberCountMock(USER_URN, 7);
        const { result } = renderUseGroupMemberCount({ urn: USER_URN, type: EntityType.CorpUser } as CorpUser, [mock]);

        expect(result.current).toBeUndefined();
        await flushQueries();
        expect(fetch).not.toHaveBeenCalled();
    });

    it("drops a fetched count once the entity it was fetched for is replaced by one that doesn't need it", async () => {
        const { mock } = memberCountMock(GROUP_URN, 5);
        const { result, waitFor, rerender } = renderUseGroupMemberCount(
            { urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup,
            [mock],
        );
        await waitFor(() => expect(result.current).toBe(5));

        rerender({ entity: { urn: USER_URN, type: EntityType.CorpUser } as CorpUser });

        expect(result.current).toBeUndefined();
    });

    it("never shows one group's count on another group while that group's count loads", async () => {
        const { mock: firstGroup } = memberCountMock(GROUP_URN, 5);
        const { mock: secondGroup } = memberCountMock(OTHER_GROUP_URN, 9, 20);
        const { result, waitFor, rerender } = renderUseGroupMemberCount(
            { urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup,
            [firstGroup, secondGroup],
        );
        await waitFor(() => expect(result.current).toBe(5));

        rerender({ entity: { urn: OTHER_GROUP_URN, type: EntityType.CorpGroup } as CorpGroup });

        expect(result.current).toBeUndefined();
        await waitFor(() => expect(result.current).toBe(9));
    });

    it('ignores a response that belongs to another group, whatever Apollo leaves in data', () => {
        vi.mocked(useGetGroupMemberCountQuery).mockReturnValueOnce({
            data: {
                corpGroup: {
                    __typename: 'CorpGroup',
                    urn: OTHER_GROUP_URN,
                    type: EntityType.CorpGroup,
                    memberCount: { __typename: 'EntityRelationshipsResult', total: 5 },
                },
            },
        } as ReturnType<typeof useGetGroupMemberCountQuery>);

        const { result } = renderUseGroupMemberCount({ urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup, []);

        expect(result.current).toBeUndefined();
    });
});
