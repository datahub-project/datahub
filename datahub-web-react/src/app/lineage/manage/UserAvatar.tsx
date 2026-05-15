import { PartitionOutlined } from '@ant-design/icons';
import { Avatar, Popover } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';

import { toLocalDateTimeString } from '@app/shared/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, EntityType } from '@types';

const LineageIcon = styled(PartitionOutlined)`
    font-size: 16px;
    margin-right: 4px;
`;

const PopoverWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const AvatarWrapper = styled.div`
    margin-right: 20px;
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
            <AvatarWrapper>
                <Avatar name={userName || ''} imageUrl={avatarPhotoUrl} type={AvatarType.user} size="sm" />
            </AvatarWrapper>
        </Popover>
    );
}
