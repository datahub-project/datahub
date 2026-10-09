import { ListDataAccessRolesQuery } from '@graphql/dataAccessRole.generated';

export type DataAccessRole = Extract<
    NonNullable<NonNullable<ListDataAccessRolesQuery['search']>['searchResults'][number]['entity']>,
    { __typename?: 'Role' }
>;

export type DataAccessRoleUser = NonNullable<
    NonNullable<NonNullable<NonNullable<DataAccessRole['actors']>['users']>[number]>['user']
>;

export type DataAccessRoleGroup = NonNullable<
    NonNullable<NonNullable<NonNullable<DataAccessRole['actors']>['groups']>[number]>['group']
>;

export function getDataAccessRoleUsers(role: DataAccessRole | null | undefined): DataAccessRoleUser[] {
    return (role?.actors?.users ?? []).flatMap((entry) => (entry?.user ? [entry.user] : []));
}

export function getDataAccessRoleGroups(role: DataAccessRole | null | undefined): DataAccessRoleGroup[] {
    return (role?.actors?.groups ?? []).flatMap((entry) => (entry?.group ? [entry.group] : []));
}
