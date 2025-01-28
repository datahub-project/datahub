import React, { useMemo } from 'react';
import { Tooltip } from '@components';
import { EntityType, UserUsageCounts } from '../../../../types.generated';
import { SpacedAvatarGroup } from '../../../shared/avatar/SpaceAvatarGroup';
import { useEntityRegistry } from '../../../useEntityRegistry';
import ActorAvatar from '../../shared/ActorAvatar';

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
