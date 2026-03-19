import React, { useMemo } from 'react';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

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

    const avatars: AvatarItemProps[] = useMemo(() => {
        return (
            usersList?.map((user) => ({
                name: entityRegistry.getDisplayName(EntityType.CorpUser, user),
                imageUrl: user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined,
                urn: user.urn,
                type: AvatarType.user,
            })) || []
        );
    }, [usersList, entityRegistry]);

    if (!usersList?.length) return <div>-</div>;
    return <AvatarStack avatars={avatars} maxToShow={max} />;
}
