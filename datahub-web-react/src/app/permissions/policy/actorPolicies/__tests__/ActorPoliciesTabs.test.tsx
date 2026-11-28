import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import GroupPoliciesTab from '@app/permissions/policy/actorPolicies/GroupPoliciesTab';
import UserPoliciesTab from '@app/permissions/policy/actorPolicies/UserPoliciesTab';
import { ListedPolicy } from '@app/permissions/policy/actorPolicies/actorPolicies.types';
import { PolicyFieldMatch, listPoliciesMatchingAny } from '@app/permissions/policy/actorPolicies/actorPolicies.utils';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetGroupRoleUrnsDocument, GetUserPolicyAccessDocument } from '@graphql/actorPolicies.generated';
import { ListPoliciesDocument } from '@graphql/policy.generated';
import { EntityType, PolicyState, PolicyType } from '@types';

const USER = 'urn:li:corpuser:u1';
const GROUP = 'urn:li:corpGroup:g1';
const ROLE = 'urn:li:dataHubRole:Editor';

function policy(name: string, actors: Partial<ListedPolicy['actors']>, state = PolicyState.Active): ListedPolicy {
    return {
        urn: `urn:li:dataHubPolicy:${name}`,
        type: PolicyType.Platform,
        name,
        description: null,
        state,
        resources: null,
        privileges: [],
        editable: true,
        actors: {
            users: null,
            groups: null,
            roles: null,
            allUsers: false,
            allGroups: false,
            resourceOwners: false,
            resourceOwnersTypes: null,
            resolvedOwnershipTypes: null,
            resolvedUsers: null,
            resolvedGroups: null,
            resolvedRoles: null,
            ...actors,
        },
    };
}

function listPoliciesMock(
    matches: PolicyFieldMatch[],
    policies: ListedPolicy[],
    total = policies.length,
): MockedResponse {
    return {
        request: { query: ListPoliciesDocument, variables: listPoliciesMatchingAny(matches) },
        result: {
            data: { listPolicies: { start: 0, count: policies.length, total, policies } },
        },
    };
}

function groupRolesMock(roleUrns: string[]): MockedResponse {
    return {
        request: { query: GetGroupRoleUrnsDocument, variables: { urn: GROUP } },
        result: {
            data: {
                corpGroup: { urn: GROUP, roles: { relationships: roleUrns.map((urn) => ({ entity: { urn } })) } },
            },
        },
    };
}

const GROUP_MATCHES: PolicyFieldMatch[] = [
    { field: 'groups', values: [GROUP] },
    { field: 'allGroups', values: ['true'] },
];

const TRUNCATION_WARNING = /Only the first/;
const LOAD_ERROR = 'Failed to load policies! An unexpected error occurred.';

function renderWithMocks(mocks: MockedResponse[], ui: React.ReactElement) {
    return render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer>{ui}</TestPageContainer>
        </MockedProvider>,
    );
}

describe('GroupPoliciesTab', () => {
    it('ORs group, all-groups and role filters in one query, without all-users policies, and explains each row', async () => {
        const mocks = [
            groupRolesMock([ROLE]),
            listPoliciesMock(
                [...GROUP_MATCHES, { field: 'roles', values: [ROLE] }],
                [
                    policy('Named group', { groups: [GROUP] }),
                    policy('Every group', { allGroups: true }),
                    policy('Editors', {
                        roles: [ROLE],
                        resolvedRoles: [{ urn: ROLE, type: EntityType.DatahubRole, name: 'Editor', description: '' }],
                    }),
                ],
            ),
        ];
        renderWithMocks(mocks, <GroupPoliciesTab urn={GROUP} />);

        expect(await screen.findByText('Named group')).toBeInTheDocument();
        expect(await screen.findByText('Every group')).toBeInTheDocument();
        expect(await screen.findByText('Editors')).toBeInTheDocument();
        expect(screen.getByText('Direct')).toBeInTheDocument();
        expect(screen.getByText('All groups')).toBeInTheDocument();
        expect(screen.getByText('Role: Editor')).toBeInTheDocument();
        expect(screen.getByText('3 policies')).toBeInTheDocument();
        expect(screen.queryByText(/asset owners/)).not.toBeInTheDocument();
        expect(screen.queryByText(TRUNCATION_WARNING)).not.toBeInTheDocument();
    });

    it('leaves out the roles filter for a group with no roles', async () => {
        const mocks = [
            groupRolesMock([]),
            listPoliciesMock(GROUP_MATCHES, [policy('Named group', { groups: [GROUP] })]),
        ];
        renderWithMocks(mocks, <GroupPoliciesTab urn={GROUP} />);

        expect(await screen.findByText('Named group')).toBeInTheDocument();
        expect(screen.getByText('1 policy')).toBeInTheDocument();
        // A query with a roles filter would have no matching mock and surface as a load error.
        expect(screen.queryByText(LOAD_ERROR)).not.toBeInTheDocument();
    });

    it('warns when the query returns fewer policies than its total', async () => {
        const mocks = [
            groupRolesMock([]),
            listPoliciesMock(GROUP_MATCHES, [policy('Named group', { groups: [GROUP] })], 1500),
        ];
        renderWithMocks(mocks, <GroupPoliciesTab urn={GROUP} />);

        expect(await screen.findByText('Named group')).toBeInTheDocument();
        expect(screen.getByText(TRUNCATION_WARNING)).toBeInTheDocument();
    });
});

describe('UserPoliciesTab', () => {
    const policies = [
        policy('Direct active', { users: [USER] }),
        policy('Direct inactive', { users: [USER] }, PolicyState.Inactive),
        policy('All groups only', { allGroups: true }),
        policy('All users policy', { allUsers: true }),
    ];

    it('loads debugAccess policies in full, hides inactive and inapplicable ones, and shows the gap note', async () => {
        const mocks = [
            {
                request: { query: GetUserPolicyAccessDocument, variables: { urn: USER } },
                result: {
                    data: { debugAccess: { policies: policies.map((p) => p.urn), groups: [], allRoles: [] } },
                },
            },
            listPoliciesMock([{ field: 'urn', values: policies.map((p) => p.urn) }], policies),
        ];
        renderWithMocks(mocks, <UserPoliciesTab urn={USER} />);

        expect(await screen.findByText('Direct active')).toBeInTheDocument();
        expect(screen.getByText('All users policy')).toBeInTheDocument();
        expect(screen.getByText('Everyone')).toBeInTheDocument();
        expect(screen.queryByText('Direct inactive')).not.toBeInTheDocument();
        expect(screen.queryByText('All groups only')).not.toBeInTheDocument();
        expect(screen.getByText('2 policies')).toBeInTheDocument();
        expect(
            screen.getByText(/Policies granted to asset owners and built-in default policies aren't listed/),
        ).toBeInTheDocument();
        expect(screen.queryByText(TRUNCATION_WARNING)).not.toBeInTheDocument();
    });

    it('warns when the policy details are truncated', async () => {
        const mocks = [
            {
                request: { query: GetUserPolicyAccessDocument, variables: { urn: USER } },
                result: { data: { debugAccess: { policies: [policies[0].urn], groups: [], allRoles: [] } } },
            },
            listPoliciesMock([{ field: 'urn', values: [policies[0].urn] }], [policies[0]], 2),
        ];
        renderWithMocks(mocks, <UserPoliciesTab urn={USER} />);

        expect(await screen.findByText('Direct active')).toBeInTheDocument();
        expect(screen.getByText(TRUNCATION_WARNING)).toBeInTheDocument();
    });

    it('shows the load error without the empty state when debugAccess fails', async () => {
        const mocks = [
            {
                request: { query: GetUserPolicyAccessDocument, variables: { urn: USER } },
                error: new Error('unauthorized'),
            },
        ];
        renderWithMocks(mocks, <UserPoliciesTab urn={USER} />);

        expect(await screen.findByText(LOAD_ERROR)).toBeInTheDocument();
        expect(screen.queryByText('No Policies!')).not.toBeInTheDocument();
    });
});
