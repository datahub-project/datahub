import { Tooltip } from '@components';
import { Avatar } from 'antd';
import React from 'react';

import ActorAvatar from '@app/entityV2/shared/ActorAvatar';
import { userExists } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, EntityType } from '@types';

export type Props = {
    users: Array<CorpUser>;
    max?: number;
    checkExistence?: boolean;
};

export default function TopUsersFacepile({ users, max, checkExistence = true }: Props) {
    const displayedUsers = users.filter((user) => userExists(user));
    const usersList = checkExistence ? displayedUsers : users;
    const entityRegistry = useEntityRegistry();
    return (
        <Avatar.Group maxCount={max}>
            {usersList?.map((user) => {
                const userName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
                return (
                    <Tooltip title={userName} showArrow={false}>
                        <ActorAvatar
                            size={26}
                            name={userName}
                            url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user.urn}`}
                            photoUrl={
                                user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined
                            }
                        />
                    </Tooltip>
                );
            })}
        </Avatar.Group>
    );
}
