import { describe, expect, it } from 'vitest';

import {
    DataAccessRole,
    getDataAccessRoleGroups,
    getDataAccessRoleUsers,
} from '@app/permissions/dataAccessRoles/dataAccessRoles.utils';

describe('getDataAccessRoleUsers', () => {
    it('returns provisioned users', () => {
        const role = {
            actors: {
                users: [{ user: { urn: 'urn:li:corpuser:alice' } }, { user: { urn: 'urn:li:corpuser:bob' } }],
            },
        } as DataAccessRole;

        expect(getDataAccessRoleUsers(role).map((user) => user.urn)).toEqual([
            'urn:li:corpuser:alice',
            'urn:li:corpuser:bob',
        ]);
    });

    it('drops missing users', () => {
        const role = {
            actors: {
                users: [{ user: { urn: 'urn:li:corpuser:alice' } }, { user: null }, null],
            },
        } as DataAccessRole;

        expect(getDataAccessRoleUsers(role).map((user) => user.urn)).toEqual(['urn:li:corpuser:alice']);
    });

    it('returns an empty array when the role has no actors', () => {
        expect(getDataAccessRoleUsers(undefined)).toEqual([]);
        expect(getDataAccessRoleUsers({} as DataAccessRole)).toEqual([]);
    });
});

describe('getDataAccessRoleGroups', () => {
    it('returns provisioned groups and drops missing ones', () => {
        const role = {
            actors: {
                groups: [{ group: { urn: 'urn:li:corpGroup:finance' } }, { group: null }],
            },
        } as DataAccessRole;

        expect(getDataAccessRoleGroups(role).map((group) => group.urn)).toEqual(['urn:li:corpGroup:finance']);
    });
});
