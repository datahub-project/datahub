import { PartitionOutlined } from '@ant-design/icons';
import { Avatar, Popover } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { CorpUser, EntityType } from '../../../types.generated';
import getAvatarColor from '../../shared/avatar/getAvatarColor';
import { toLocalDateTimeString } from '../../shared/time/timeUtils';
import { useEntityRegistry } from '../../useEntityRegistry';

const StyledAvatar = styled(Avatar)<{ backgroundColor: string }>`
    color: #fff;
    background-color: ${(props) => props.backgroundColor};
    margin-right: 20px;
    height: 22px;
    width: 22px;
    .ant-avatar-string {
        text-align: center;
        line-height: 22px;
    }
`;

const LineageIcon = styled(PartitionOutlined)`
    font-size: 16px;
    margin-right: 4px;
`;

const PopoverWrapper = styled.div`
    display: flex;
    align-items: center;
`;

interface Props {
    createdActor?: CorpUser | null;
    createdOn?: number | null;
}

export default function UserAvatar({ createdActor, createdOn }: Props) {
    const entityRegistry = useEntityRegistry();
    const avatarPhotoUrl = createdActor?.editableProperties?.pictureLink;
    const userName = entityRegistry.getDisplayName(EntityType.CorpUser, createdActor);

    return (
        <Popover
            content={
                <PopoverWrapper>
                    <LineageIcon /> Relationship added by&nbsp;<strong>{userName}</strong>&nbsp;
                    {createdOn && (
                        <>
                            on <strong>{toLocalDateTimeString(createdOn)}</strong>
                        </>
                    )}
                </PopoverWrapper>
            }
        >
            <StyledAvatar src={avatarPhotoUrl} backgroundColor={getAvatarColor(userName)}>
                {userName.charAt(0).toUpperCase()}
            </StyledAvatar>
        </Popover>
    );
}
