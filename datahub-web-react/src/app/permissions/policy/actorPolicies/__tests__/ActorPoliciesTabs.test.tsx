import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import GroupPoliciesTab from '@app/permissions/policy/actorPolicies/GroupPoliciesTab';
import UserPoliciesTab from '@app/permissions/policy/actorPolicies/UserPoliciesTab';
import { ListedPolicy } from '@app/permissions/policy/actorPolicies/actorPolicies.types';
import { listPoliciesByField } from '@app/permissions/policy/actorPolicies/actorPolicies.utils';
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

function listPoliciesMock(field: string, values: string[], policies: ListedPolicy[]): MockedResponse {
    return {
        request: { query: ListPoliciesDocument, variables: listPoliciesByField(field, values) },
        result: {
            data: { listPolicies: { start: 0, count: policies.length, total: policies.length, policies } },
        },
    };
}

function renderWithMocks(mocks: MockedResponse[], ui: React.ReactElement) {
    return render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer>{ui}</TestPageContainer>
        </MockedProvider>,
    );
}

describe('GroupPoliciesTab', () => {
    it('merges group, all-groups and role queries, without all-users policies, and explains each row', async () => {
        const mocks = [
            {
                request: { query: GetGroupRoleUrnsDocument, variables: { urn: GROUP } },
                result: {
                    data: { corpGroup: { urn: GROUP, roles: { relationships: [{ entity: { urn: ROLE } }] } } },
                },
            },
            listPoliciesMock('groups', [GROUP], [policy('Named group', { groups: [GROUP] })]),
            listPoliciesMock('allGroups', ['true'], [policy('Every group', { allGroups: true })]),
            listPoliciesMock(
                'roles',
                [ROLE],
                [
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
            listPoliciesMock(
                'urn',
                policies.map((p) => p.urn),
                policies,
            ),
        ];
        renderWithMocks(mocks, <UserPoliciesTab urn={USER} />);

        expect(await screen.findByText('Direct active')).toBeInTheDocument();
        expect(screen.getByText('All users policy')).toBeInTheDocument();
        expect(screen.getByText('Everyone')).toBeInTheDocument();
        expect(screen.queryByText('Direct inactive')).not.toBeInTheDocument();
        expect(screen.queryByText('All groups only')).not.toBeInTheDocument();
        expect(screen.getByText('2 policies')).toBeInTheDocument();
        expect(screen.getByText(/Policies granted to asset owners aren't listed/)).toBeInTheDocument();
    });
});
