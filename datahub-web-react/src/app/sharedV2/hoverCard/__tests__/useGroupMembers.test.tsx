import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import useGroupMembers, { HOVER_CARD_MEMBER_LIMIT } from '@app/sharedV2/hoverCard/useGroupMembers';

import { GetGroupHoverCardMembersDocument, useGetGroupHoverCardMembersQuery } from '@graphql/group.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

// Passes through to the real query; one test swaps in a stale response to pin the urn check.
vi.mock('@graphql/group.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/group.generated')>();
    return { ...actual, useGetGroupHoverCardMembersQuery: vi.fn(actual.useGetGroupHoverCardMembersQuery) };
});

const GROUP_URN = 'urn:li:corpGroup:data-platform';
const OTHER_GROUP_URN = 'urn:li:corpGroup:analytics';
const USER_URN = 'urn:li:corpuser:jdoe';

function memberUser(username: string) {
    return {
        __typename: 'CorpUser',
        urn: `urn:li:corpuser:${username}`,
        type: EntityType.CorpUser,
        username,
        info: null,
        properties: null,
        editableProperties: null,
    };
}

function membersMock(urn: string, total: number, usernames: string[] = [], delay = 0) {
    const result = vi.fn(() => ({
        data: {
            corpGroup: {
                __typename: 'CorpGroup',
                urn,
                type: EntityType.CorpGroup,
                memberCount: {
                    __typename: 'EntityRelationshipsResult',
                    total,
                    relationships: usernames.map((username) => ({
                        __typename: 'EntityRelationship',
                        entity: memberUser(username),
                    })),
                },
            },
        },
    }));
    const mock: MockedResponse = {
        request: { query: GetGroupHoverCardMembersDocument, variables: { urn, count: HOVER_CARD_MEMBER_LIMIT } },
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

function renderUseGroupMembers(entity: Entity, mocks: MockedResponse[]) {
    return renderHook((props: { entity: Entity }) => useGroupMembers(props.entity), {
        initialProps: { entity },
        wrapper: ({ children }) => <MockedProvider mocks={mocks}>{children}</MockedProvider>,
    });
}

describe('useGroupMembers', () => {
    it('fetches the total and first members of a group', async () => {
        const { mock } = membersMock(GROUP_URN, 7, ['alice', 'bob']);
        const { result, waitFor } = renderUseGroupMembers({ urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup, [
            mock,
        ]);

        expect(result.current.total).toBeUndefined();
        await waitFor(() => expect(result.current.total).toBe(7));
        expect(result.current.members.map((member) => member.username)).toEqual(['alice', 'bob']);
    });

    it('shows a total the group was loaded with until the fetch lands', async () => {
        const { mock } = membersMock(GROUP_URN, 7, ['alice']);
        const { result, waitFor } = renderUseGroupMembers(
            { urn: GROUP_URN, type: EntityType.CorpGroup, memberCount: { total: 6 } } as unknown as CorpGroup,
            [mock],
        );

        expect(result.current.total).toBe(6);
        await waitFor(() => expect(result.current.total).toBe(7));
    });

    it('does not trust a plain relationships total, which may count one kind of membership', () => {
        const { result } = renderUseGroupMembers(
            { urn: GROUP_URN, type: EntityType.CorpGroup, relationships: { total: 3 } } as unknown as CorpGroup,
            [membersMock(GROUP_URN, 7).mock],
        );

        expect(result.current.total).toBeUndefined();
    });

    it('returns nothing for entities other than groups, without fetching', async () => {
        const { mock, result: fetch } = membersMock(USER_URN, 7);
        const { result } = renderUseGroupMembers({ urn: USER_URN, type: EntityType.CorpUser } as CorpUser, [mock]);

        expect(result.current).toEqual({ members: [] });
        await flushQueries();
        expect(fetch).not.toHaveBeenCalled();
    });

    it("never shows one group's members on another group while that group's members load", async () => {
        const { mock: firstGroup } = membersMock(GROUP_URN, 5, ['alice']);
        const { mock: secondGroup } = membersMock(OTHER_GROUP_URN, 9, ['carol'], 20);
        const { result, waitFor, rerender } = renderUseGroupMembers(
            { urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup,
            [firstGroup, secondGroup],
        );
        await waitFor(() => expect(result.current.total).toBe(5));

        rerender({ entity: { urn: OTHER_GROUP_URN, type: EntityType.CorpGroup } as CorpGroup });

        expect(result.current).toEqual({ total: undefined, members: [] });
        await waitFor(() => expect(result.current.total).toBe(9));
        expect(result.current.members.map((member) => member.username)).toEqual(['carol']);
    });

    it('ignores a response that belongs to another group, whatever Apollo leaves in data', () => {
        vi.mocked(useGetGroupHoverCardMembersQuery).mockReturnValueOnce({
            data: {
                corpGroup: {
                    __typename: 'CorpGroup',
                    urn: OTHER_GROUP_URN,
                    type: EntityType.CorpGroup,
                    memberCount: { __typename: 'EntityRelationshipsResult', total: 5, relationships: [] },
                },
            },
        } as unknown as ReturnType<typeof useGetGroupHoverCardMembersQuery>);

        const { result } = renderUseGroupMembers({ urn: GROUP_URN, type: EntityType.CorpGroup } as CorpGroup, []);

        expect(result.current).toEqual({ total: undefined, members: [] });
    });
});
