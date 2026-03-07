import React, { useMemo } from 'react';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { UserUsageCounts } from '@types';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
    maxNumberDisplayed?: number;
};

export default function UsageFacepile({ users, maxNumberDisplayed }: Props) {
    const sortedUsers = useMemo(() => users?.slice().sort((a, b) => (b?.count || 0) - (a?.count || 0)), [users]);

    const avatars: AvatarItemProps[] = useMemo(() => {
        let displayed = sortedUsers;
        if (maxNumberDisplayed) {
            displayed = displayed?.slice(0, maxNumberDisplayed);
        }
        return (
            displayed?.map((user) => ({
                name: user?.userEmail || '',
                type: AvatarType.user,
            })) || []
        );
    }, [sortedUsers, maxNumberDisplayed]);

    return <AvatarStack avatars={avatars} maxToShow={2} />;
}
