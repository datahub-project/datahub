/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    const entityRegistry = useEntityRegistry();
    const displayedUsers = users.filter((user) => userExists(user));
    const usersList = checkExistence ? displayedUsers : users;
    if (!usersList?.length) return <div>-</div>;
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
