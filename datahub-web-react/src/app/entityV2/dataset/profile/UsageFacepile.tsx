/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React, { useMemo } from 'react';

import ActorAvatar from '@app/entityV2/shared/ActorAvatar';
import { SpacedAvatarGroup } from '@app/shared/avatar/SpaceAvatarGroup';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, UserUsageCounts } from '@types';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
    maxNumberDisplayed?: number;
};

export default function UsageFacepile({ users, maxNumberDisplayed }: Props) {
    const sortedUsers = useMemo(() => users?.slice().sort((a, b) => (b?.count || 0) - (a?.count || 0)), [users]);
    let displayedUsers = sortedUsers;
    if (maxNumberDisplayed) {
        displayedUsers = displayedUsers?.slice(0, maxNumberDisplayed);
    }

    const entityRegistry = useEntityRegistry();

    return (
        <SpacedAvatarGroup maxCount={2}>
            {displayedUsers?.map((displayedUser) => {
                const user = displayedUser?.user;
                const userName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
                return (
                    <Tooltip title={userName}>
                        <ActorAvatar
                            size={32}
                            name={userName}
                            url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user?.urn}`}
                            photoUrl={
                                user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined
                            }
                        />
                    </Tooltip>
                );
            })}
        </SpacedAvatarGroup>
    );
}
