import React from 'react';
import { Avatar, Tooltip } from 'antd';
import { UserUsageCounts } from '../../../../types.generated';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
};

export default function UsageFacepile({ users }: Props) {
    return (
        <Avatar.Group maxCount={2}>
            {users?.map((user) => (
                <Tooltip title={user?.userEmail}>
                    <Avatar>{user?.userEmail?.charAt(0).toUpperCase()}</Avatar>
                </Tooltip>
            ))}
        </Avatar.Group>
    );
}
