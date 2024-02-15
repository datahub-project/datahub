import React from 'react';
import { Avatar, Tooltip } from 'antd';
import { CorpUser, EntityType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import ActorAvatar from '../../../../ActorAvatar';
import { userExists } from './utils';

export type Props = {
    users: Array<CorpUser>;
    max?: number;
};

export default function TopUsersFacepile({ users, max }: Props) {
    const displayedUsers = users.filter((user) => userExists(user));
    const entityRegistry = useEntityRegistry();
    return (
        <Avatar.Group maxCount={max}>
            {displayedUsers?.map((user) => {
                const userName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
                return (
                    <Tooltip title={userName}>
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
