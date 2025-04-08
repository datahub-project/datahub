import { LockOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { MemberCount } from '@app/entityV2/group/GroupSidebar';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

import { EntityRelationshipsResult } from '@types';

const GroupHeader = styled.div`
    position: relative;
    z-index: 2;
`;

const GroupName = styled(Typography.Title)`
    word-wrap: break-word;
    text-align: left;
    &&& {
        margin-bottom: 0;
        word-break: break-all;
        font-size: 12px;
        color: ${REDESIGN_COLORS.WHITE};
        text-overflow: ellipsis;
        overflow: hidden;
        white-space: nowrap;
    }

    .ant-typography-edit {
        font-size: 12px;
    }
`;

type Props = {
    groupMemberRelationships: EntityRelationshipsResult;
    isExternalGroup: boolean;
    externalGroupType: string | undefined;
    groupName: string | undefined;
};

export const GroupInfoHeaderSection = ({
    groupMemberRelationships,
    externalGroupType,
    isExternalGroup,
    groupName,
}: Props) => {
    const groupMemberRelationshipsTotal = groupMemberRelationships?.total || 0;
    return (
        <GroupHeader>
            <Tooltip title={groupName}>
                <GroupName level={3}>{groupName}</GroupName>
            </Tooltip>
            {groupMemberRelationshipsTotal > 0 && <MemberCount>{groupMemberRelationshipsTotal} members</MemberCount>}
            {isExternalGroup && (
                <Tooltip
                    title={`Membership for this group cannot be edited in DataHub as it originates from ${externalGroupType}.`}
                >
                    <LockOutlined />
                </Tooltip>
            )}
        </GroupHeader>
    );
};
