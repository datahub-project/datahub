import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import { print } from 'graphql';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { PreviewType } from '@app/entityV2/Entity';
import { GroupEntity } from '@app/entityV2/group/Group';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { OwnershipFieldsFragmentDoc } from '@graphql/fragments.generated';
import { CorpGroup, EntityType } from '@types';

// A group that only carries the members list fetched by the owner fragment (no member-count
// alias). This mirrors what the owner hover card receives.
const groupWithMembersList = {
    urn: 'urn:li:corpGroup:test-group',
    type: EntityType.CorpGroup,
    name: 'Test Group',
    info: {
        members: [{ urn: 'urn:li:corpuser:member-a' }, { urn: 'urn:li:corpuser:member-b' }],
    },
} as unknown as CorpGroup;

// A group that carries the authoritative member-count total from the relationships alias.
const groupWithMemberCount = {
    urn: 'urn:li:corpGroup:test-group',
    type: EntityType.CorpGroup,
    name: 'Test Group',
    memberCount: { total: 5 },
    info: {
        members: [{ urn: 'urn:li:corpuser:member-a' }],
    },
} as unknown as CorpGroup;

// A genuinely empty group whose authoritative total is 0 but that still carries a stale
// members list. The count must reflect the authoritative 0, not the list length.
const emptyGroupWithStaleMembersList = {
    urn: 'urn:li:corpGroup:test-group',
    type: EntityType.CorpGroup,
    name: 'Test Group',
    memberCount: { total: 0 },
    info: {
        members: [{ urn: 'urn:li:corpuser:member-a' }, { urn: 'urn:li:corpuser:member-b' }],
    },
} as unknown as CorpGroup;

const renderGroupPreview = (group: CorpGroup) =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>{new GroupEntity().renderPreview(PreviewType.PREVIEW, group)}</TestPageContainer>
        </MockedProvider>,
    );

describe('GroupEntity preview member count', () => {
    it('renders the real member count for a group that has members', () => {
        renderGroupPreview(groupWithMembersList);
        expect(screen.getByText('2 members')).toBeInTheDocument();
    });

    it('prefers the authoritative member-count total when present', () => {
        renderGroupPreview(groupWithMemberCount);
        expect(screen.getByText('5 members')).toBeInTheDocument();
    });

    it('shows an authoritative zero instead of falling through to a stale members list', () => {
        renderGroupPreview(emptyGroupWithStaleMembersList);
        expect(screen.getByText('0 members')).toBeInTheDocument();
        expect(screen.queryByText('2 members')).not.toBeInTheDocument();
    });
});

describe('ownershipFields fragment', () => {
    // Guards the actual query selection: the render tests above pass hand-built data and would stay
    // green even if the fragment stopped fetching the count. This fails if the owner fragment no
    // longer pulls the group member count that the hover card renders.
    it('selects the corpGroup member count for owner hover cards', () => {
        const printed = print(OwnershipFieldsFragmentDoc);
        expect(printed).toContain('memberCount');
        expect(printed).toContain('IsMemberOfGroup');
        expect(printed).toContain('IsMemberOfNativeGroup');
    });
});
