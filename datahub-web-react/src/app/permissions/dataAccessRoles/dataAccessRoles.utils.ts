import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { ListDataAccessRolesQuery } from '@graphql/dataAccessRole.generated';
import { EntityType } from '@types';

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

type EntityRegistry = ReturnType<typeof useEntityRegistry>;

export function getDataAccessRoleUserAvatars(
    role: DataAccessRole | null | undefined,
    entityRegistry: EntityRegistry,
): AvatarItemProps[] {
    return getDataAccessRoleUsers(role).map((user) => ({
        name: entityRegistry.getDisplayName(EntityType.CorpUser, user),
        imageUrl: user.editableProperties?.pictureLink || undefined,
        urn: user.urn,
        type: AvatarType.user,
    }));
}

export function getDataAccessRoleGroupAvatars(
    role: DataAccessRole | null | undefined,
    entityRegistry: EntityRegistry,
): AvatarItemProps[] {
    return getDataAccessRoleGroups(role).map((group) => ({
        name: entityRegistry.getDisplayName(EntityType.CorpGroup, group),
        imageUrl: group.editableProperties?.pictureLink || undefined,
        urn: group.urn,
        type: AvatarType.group,
    }));
}
