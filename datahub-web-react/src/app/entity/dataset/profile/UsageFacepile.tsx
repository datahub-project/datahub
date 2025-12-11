/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
