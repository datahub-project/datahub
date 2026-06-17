import { Avatar } from '@components';
import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';

import {
    getDescriptionFromType,
    getNameFromType,
} from '@app/entity/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Owner } from '@types';

const TextWrapper = styled.span<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

const ContentWrapper = styled.span`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const OwnerPopoverTitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-start;
    gap: 6px;
`;

const OwnerEntityTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 500;
    font-size: 14px;
    line-height: 19px;
`;

const OwnerPopoverContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
`;

const OwnerNameText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 700;
    font-size: 12px;
    line-height: 16px;
    margin-bottom: 4px;
`;

const OwnershipTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 700;
    font-size: 10px;
    line-height: 14px;
`;

const OwnwershipTypeDescriptionText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 500;
    font-size: 10px;
    line-height: 14px;
`;

interface Props {
    name: string;
    owner: Owner;
    hidePopOver?: boolean;
    pictureLink?: string;
    fontSize?: number;
}

export default function OwnerContent({ name, owner, hidePopOver, pictureLink, fontSize }: Props) {
    const entityRegistry = useEntityRegistry();
    const ownerEntity = owner.owner;
    const ownerEntityType = owner.owner.type;
    const ownerEntityTypeDisplayName = entityRegistry.getEntityName(ownerEntityType);
    const ownerDisplayName = entityRegistry.getDisplayName(ownerEntityType, ownerEntity);
    const avatarType = ownerEntityType === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;
    let ownershipTypeName;
    let ownershipTypeDescription;
    if (owner.ownershipType && owner.ownershipType.info) {
        ownershipTypeName = owner.ownershipType.info.name;
        ownershipTypeDescription = owner.ownershipType.info.description;
    } else if (owner.type) {
        ownershipTypeName = getNameFromType(owner.type);
        ownershipTypeDescription = getDescriptionFromType(owner.type);
    }

    const avatar = <Avatar name={name} imageUrl={pictureLink} type={avatarType} />;

    return (
        <ContentWrapper>
            {avatar}
            {hidePopOver ? (
                <TextWrapper fontSize={fontSize}>{name}</TextWrapper>
            ) : (
                <Popover
                    overlayStyle={{ maxWidth: 200 }}
                    placement="left"
                    title={
                        <OwnerPopoverTitleContainer>
                            {avatar}
                            <OwnerEntityTypeText>{ownerEntityTypeDisplayName}</OwnerEntityTypeText>
                        </OwnerPopoverTitleContainer>
                    }
                    content={
                        <OwnerPopoverContentContainer>
                            <OwnerNameText>{ownerDisplayName}</OwnerNameText>
                            <OwnershipTypeText>{ownershipTypeName}</OwnershipTypeText>
                            <OwnwershipTypeDescriptionText>{ownershipTypeDescription}</OwnwershipTypeDescriptionText>
                        </OwnerPopoverContentContainer>
                    }
                >
                    <TextWrapper fontSize={fontSize}>{name}</TextWrapper>
                </Popover>
            )}
        </ContentWrapper>
    );
}
