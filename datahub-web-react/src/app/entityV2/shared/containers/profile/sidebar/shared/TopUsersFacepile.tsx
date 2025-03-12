import React from 'react';
import { Avatar } from 'antd';
import { Tooltip } from '@components';
import { CorpUser, EntityType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import ActorAvatar from '../../../../ActorAvatar';
import { userExists } from './utils';

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
