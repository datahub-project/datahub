import React from 'react';

import EntityRegistry from '@app/entity/EntityRegistry';
import { CustomAvatar } from '@app/shared/avatar';
import { SpacedAvatarGroup } from '@app/shared/avatar/SpaceAvatarGroup';

import { CorpGroup, CorpUser, DataHubPolicy, DataHubRole, EntityType, Maybe } from '@types';

type Props = {
    users?: Maybe<Array<CorpUser>>;
    groups?: Maybe<Array<CorpGroup>>;
    policies?: Maybe<Array<DataHubPolicy>>;
    roles?: Maybe<Array<DataHubRole>>;
    entityRegistry: EntityRegistry;
    maxCount?: number;
    size?: number;
};

/**
 * Component used for displaying the users and groups in policies table
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export default function AvatarsGroup({ users, groups, policies, roles, entityRegistry, maxCount = 6, size }: Props) {
    return (
        <SpacedAvatarGroup maxCount={maxCount}>
            {users &&
                users?.length > 0 &&
                users?.map((user, key) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <div data-testid={`avatar-tag-${user?.urn}`} key={`${user?.urn}-${key}`}>
                        <CustomAvatar
                            size={size}
                            name={entityRegistry.getDisplayName(EntityType.CorpUser, user)}
                            url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user?.urn}`}
                            photoUrl={
                                user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined
                            }
                        />
                    </div>
                ))}
            {groups &&
                groups.length > 0 &&
                groups?.map((group, key) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <div data-testid={`avatar-tag-${group.urn}`} key={`${group.urn}-${key}`}>
                        <CustomAvatar
                            size={size}
                            name={entityRegistry.getDisplayName(EntityType.CorpGroup, group)}
                            url={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${group.urn}`}
                            isGroup
                        />
                    </div>
                ))}
            {roles &&
                roles.length > 0 &&
                roles?.map((role, key) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <div data-testid={`avatar-tag-${role.urn}`} key={`${role.urn}-${key}`}>
                        <CustomAvatar size={size} name={role.name} isRole />
                    </div>
                ))}
            {policies &&
                policies.length > 0 &&
                policies?.map((policy, key) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <div data-testid={`avatar-tag-${policy.urn}`} key={`${policy.urn}-${key}`}>
                        <CustomAvatar size={size} name={policy.name} isPolicy />
                    </div>
                ))}
        </SpacedAvatarGroup>
    );
}
