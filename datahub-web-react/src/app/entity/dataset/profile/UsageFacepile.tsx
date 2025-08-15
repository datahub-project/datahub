import { Avatar, Tooltip } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { SpacedAvatarGroup } from '@app/shared/avatar/SpaceAvatarGroup';
import getAvatarColor from '@app/shared/avatar/getAvatarColor';

import { UserUsageCounts } from '@types';

export type Props = {
    users?: (UserUsageCounts | null)[] | null;
    maxNumberDisplayed?: number;
};

const AvatarStyled = styled(Avatar)<{ backgroundColor: string }>`
    color: #fff;
    background-color: ${(props) => props.backgroundColor};
`;

export default function UsageFacepile({ users, maxNumberDisplayed }: Props) {
    const sortedUsers = useMemo(() => users?.slice().sort((a, b) => (b?.count || 0) - (a?.count || 0)), [users]);
    let displayedUsers = sortedUsers;
    if (maxNumberDisplayed) {
        displayedUsers = displayedUsers?.slice(0, maxNumberDisplayed);
    }

    return (
        <SpacedAvatarGroup maxCount={2}>
            {displayedUsers?.map((user) => (
                <Tooltip title={user?.userEmail}>
                    <AvatarStyled backgroundColor={getAvatarColor(user?.userEmail || undefined)}>
                        {user?.userEmail?.charAt(0).toUpperCase()}
                    </AvatarStyled>
                </Tooltip>
            ))}
        </SpacedAvatarGroup>
    );
}
