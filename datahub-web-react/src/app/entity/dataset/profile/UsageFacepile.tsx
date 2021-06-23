import React from 'react';
import { Avatar } from 'antd';
import { UserUsageCounts } from '../../../../types.generated';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
};

export default function UsageFacepile({ users }: Props) {
    return (
        <Avatar.Group maxCount={3}>
            {users?.map((user) => (
                <Avatar>{user?.userEmail?.charAt(0).toUpperCase()}</Avatar>
            ))}
        </Avatar.Group>
    );
}
