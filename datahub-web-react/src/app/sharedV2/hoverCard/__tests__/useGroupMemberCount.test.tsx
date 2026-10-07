import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import useGroupMemberCount from '@app/sharedV2/hoverCard/useGroupMemberCount';

import { GetGroupMemberCountDocument } from '@graphql/group.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

const GROUP_URN = 'urn:li:corpGroup:data-platform';
const USER_URN = 'urn:li:corpuser:jdoe';

function memberCountMock(urn: string, total: number) {
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
    const mock: MockedResponse = { request: { query: GetGroupMemberCountDocument, variables: { urn } }, result };
    return { mock, result };
}

/** Lets a query that was (wrongly) started reach its mock before asserting it never ran. */
function flushQueries() {
    return new Promise((resolve) => {
        setTimeout(resolve, 0);
    });
}

function renderUseGroupMemberCount(entity: Entity, mocks: MockedResponse[]) {
    return renderHook(() => useGroupMemberCount(entity), {
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

    it('returns nothing for entities other than groups, without fetching', async () => {
        const { mock, result: fetch } = memberCountMock(USER_URN, 7);
        const { result } = renderUseGroupMemberCount({ urn: USER_URN, type: EntityType.CorpUser } as CorpUser, [mock]);

        expect(result.current).toBeUndefined();
        await flushQueries();
        expect(fetch).not.toHaveBeenCalled();
    });
});
