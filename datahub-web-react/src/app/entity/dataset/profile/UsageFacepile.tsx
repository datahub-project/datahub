import React, { useMemo } from 'react';
import { Avatar, Tooltip } from 'antd';
import { UserUsageCounts } from '../../../../types.generated';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
};

export default function UsageFacepile({ users }: Props) {
    const sortedUsers = useMemo(() => users?.slice().sort((a, b) => (b?.count || 0) - (a?.count || 0)), [users]);

    return (
        <Avatar.Group maxCount={2}>
            {sortedUsers?.map((user) => (
                <Tooltip title={user?.userEmail}>
                    <Avatar>{user?.userEmail?.charAt(0).toUpperCase()}</Avatar>
                </Tooltip>
            ))}
        </Avatar.Group>
    );
}
