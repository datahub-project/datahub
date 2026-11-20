import { ListedPolicy } from '@app/permissions/policy/actorPolicies/actorPolicies.types';
import {
    ActorPolicyContext,
    buildActorPolicyRows,
    filterRowsByState,
    getActorPoliciesVisibility,
} from '@app/permissions/policy/actorPolicies/actorPolicies.utils';

import { PolicyState, PolicyType } from '@types';

const USER = 'urn:li:corpuser:u1';
const GROUP = 'urn:li:corpGroup:g1';
const OTHER_GROUP = 'urn:li:corpGroup:g2';
const ROLE = 'urn:li:dataHubRole:Reader';

function policy(urn: string, actors: Partial<ListedPolicy['actors']>, state = PolicyState.Active): ListedPolicy {
    return {
        urn,
        type: PolicyType.Platform,
        name: urn,
        state,
        privileges: [],
        editable: true,
        resources: null,
        description: null,
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

const userContext = (groupUrns: string[]): ActorPolicyContext => ({
    actorType: 'user',
    actorUrn: USER,
    groupUrns,
    roleUrns: [ROLE],
});

describe('buildActorPolicyRows', () => {
    it('explains why each policy applies to a user', () => {
        const rows = buildActorPolicyRows(
            [
                policy('p:direct', { users: [USER] }),
                policy('p:group', { groups: [GROUP, OTHER_GROUP] }),
                policy('p:role', { roles: [ROLE] }),
                policy('p:everyone', { allUsers: true }),
                policy('p:allGroups', { allGroups: true }),
            ],
            userContext([GROUP]),
        );
        expect(rows.map((r) => [r.policy.urn, r.reasons])).toEqual([
            ['p:direct', [{ type: 'direct' }]],
            ['p:group', [{ type: 'group', urn: GROUP }]],
            ['p:role', [{ type: 'role', urn: ROLE }]],
            ['p:everyone', [{ type: 'everyone' }]],
            ['p:allGroups', [{ type: 'allGroups' }]],
        ]);
    });

    it('drops all-groups-only policies when the user is in no group, but keeps other matches', () => {
        const rows = buildActorPolicyRows(
            [policy('p:allGroups', { allGroups: true }), policy('p:mixed', { allGroups: true, users: [USER] })],
            userContext([]),
        );
        expect(rows.map((r) => [r.policy.urn, r.reasons])).toEqual([['p:mixed', [{ type: 'direct' }]]]);
    });

    it('explains group policies without the all-users reason, and dedupes', () => {
        const groupContext: ActorPolicyContext = {
            actorType: 'group',
            actorUrn: GROUP,
            groupUrns: [],
            roleUrns: [ROLE],
        };
        const both = policy('p:both', { groups: [GROUP], allGroups: true, allUsers: true });
        const rows = buildActorPolicyRows([both, policy('p:role', { roles: [ROLE] }), both], groupContext);
        expect(rows.map((r) => [r.policy.urn, r.reasons])).toEqual([
            ['p:both', [{ type: 'direct' }, { type: 'allGroups' }]],
            ['p:role', [{ type: 'role', urn: ROLE }]],
        ]);
    });
});

describe('filterRowsByState', () => {
    const rows = buildActorPolicyRows(
        [policy('p:on', { users: [USER] }), policy('p:off', { users: [USER] }, PolicyState.Inactive)],
        userContext([]),
    );
    it.each([
        ['ACTIVE', ['p:on']],
        ['INACTIVE', ['p:off']],
        ['ALL', ['p:on', 'p:off']],
    ] as const)('%s', (status, expected) => {
        expect(filterRowsByState(rows, status).map((r) => r.policy.urn)).toEqual(expected);
    });
});

describe('getActorPoliciesVisibility', () => {
    it.each([
        [undefined, false, false],
        [{ managePolicies: false, manageIdentities: false }, false, false],
        [{ managePolicies: true, manageIdentities: false }, true, false],
        [{ managePolicies: false, manageIdentities: true }, false, false],
        [{ managePolicies: true, manageIdentities: true }, true, true],
    ])('%o', (privileges, showGroupPolicies, showUserPolicies) => {
        expect(getActorPoliciesVisibility(privileges)).toEqual({ showGroupPolicies, showUserPolicies });
    });
});
