import React, { useMemo } from 'react';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, UserUsageCounts } from '@types';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
    maxNumberDisplayed?: number;
};

export default function UsageFacepile({ users, maxNumberDisplayed }: Props) {
    const entityRegistry = useEntityRegistry();
    const sortedUsers = useMemo(() => users?.slice().sort((a, b) => (b?.count || 0) - (a?.count || 0)), [users]);

    const avatars: AvatarItemProps[] = useMemo(() => {
        let displayed = sortedUsers;
        if (maxNumberDisplayed) {
            displayed = displayed?.slice(0, maxNumberDisplayed);
        }
        return (
            displayed?.map((displayedUser) => {
                const user = displayedUser?.user;
                return {
                    name: entityRegistry.getDisplayName(EntityType.CorpUser, user),
                    imageUrl: user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined,
                    urn: user?.urn,
                    type: AvatarType.user,
                };
            }) || []
        );
    }, [sortedUsers, maxNumberDisplayed, entityRegistry]);

    return <AvatarStack avatars={avatars} maxToShow={2} />;
}
