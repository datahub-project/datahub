import { Avatar, Tooltip, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType, Owner } from '@types';

const Details = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 500;
`;

const OwnerName = styled.div`
    width: 110px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

interface Props {
    owner: Owner;
}

const OwnerDetail = ({ owner }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const ownerName = entityRegistry.getDisplayName(EntityType.CorpUser, owner.owner);
    const ownerPictureLink = owner.owner.editableProperties?.pictureLink || undefined;
    const avatarType = owner.owner.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

    return (
        <>
            {!!ownerName && (
                <Details>
                    <Avatar name={ownerName} imageUrl={ownerPictureLink} type={avatarType} />
                    <Tooltip title={ownerName}>
                        <OwnerName>{ownerName}</OwnerName>
                    </Tooltip>
                </Details>
            )}
        </>
    );
};

export default OwnerDetail;
