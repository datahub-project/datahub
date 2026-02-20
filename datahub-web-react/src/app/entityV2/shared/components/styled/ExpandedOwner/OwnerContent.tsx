import { Avatar, Popover, colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';

import {
    getDescriptionFromType,
    getNameFromType,
} from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Owner } from '@types';

const TextWrapper = styled.span<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    color: ${colors.gray[1700]};

    max-width: 150px;
    text-overflow: ellipsis;
    overflow: hidden;
`;

const ContentWrapper = styled.span`
    display: flex;
    align-items: center;
    gap: 4px;

    max-width: inherit;
`;

const OwnerPopoverTitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-start;
    gap: 6px;
`;

const OwnerEntityTypeText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-weight: 500;
    font-size: 14px;
    line-height: 19px;
`;

const OwnerPopoverContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;

    text-overflow: ellipsis;
    overflow: hidden;
`;

const OwnerNameText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-weight: 700;
    font-size: 12px;
    line-height: 16px;
    margin-bottom: 4px;
`;

const OwnershipTypeText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-weight: 700;
    font-size: 10px;
    line-height: 14px;
`;

const OwnwershipTypeDescriptionText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
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
    dataTestId?: string;
}

export default function OwnerContent({ name, owner, hidePopOver, pictureLink, fontSize, dataTestId }: Props) {
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
        <ContentWrapper data-testid={dataTestId}>
            {avatar}
            {hidePopOver ? (
                <TextWrapper fontSize={fontSize}>{name}</TextWrapper>
            ) : (
                <Popover
                    overlayStyle={{ maxWidth: 200 }}
                    placement="left"
                    showArrow={false}
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
