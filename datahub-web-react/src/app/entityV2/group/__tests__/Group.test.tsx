import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { PreviewType } from '@app/entityV2/Entity';
import { GroupEntity } from '@app/entityV2/group/Group';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

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

describe('GroupEntity preview member count', () => {
    it('renders the real member count for a group that has members', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    {new GroupEntity().renderPreview(PreviewType.PREVIEW, groupWithMembersList)}
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByText('2 members')).toBeInTheDocument();
    });

    it('prefers the authoritative member-count total when present', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    {new GroupEntity().renderPreview(PreviewType.PREVIEW, groupWithMemberCount)}
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByText('5 members')).toBeInTheDocument();
    });
});
