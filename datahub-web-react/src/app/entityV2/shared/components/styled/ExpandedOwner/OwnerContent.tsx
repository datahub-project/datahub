import React from 'react';
import { Typography } from 'antd';
import { Popover } from '@components';
import styled from 'styled-components/macro';
import { Owner } from '../../../../../../types.generated';
import { CustomAvatar } from '../../../../../shared/avatar';
import { getDescriptionFromType, getNameFromType } from '../../../containers/profile/sidebar/Ownership/ownershipUtils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { REDESIGN_COLORS } from '../../../constants';

const TextWrapper = styled.span<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    color: ${REDESIGN_COLORS.DARK_GREY};

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

const AvatarWrapper = styled.div`
    min-width: 24px;
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
}

export default function OwnerContent({ name, owner, hidePopOver, pictureLink, fontSize }: Props) {
    const entityRegistry = useEntityRegistry();
    const ownerEntity = owner.owner;
    const ownerEntityType = owner.owner.type;
    const ownerEntityTypeDisplayName = entityRegistry.getEntityName(ownerEntityType);
    const ownerDisplayName = entityRegistry.getDisplayName(ownerEntityType, ownerEntity);
    let ownershipTypeName;
    let ownershipTypeDescription;
    if (owner.ownershipType && owner.ownershipType.info) {
        ownershipTypeName = owner.ownershipType.info.name;
        ownershipTypeDescription = owner.ownershipType.info.description;
    } else if (owner.type) {
        ownershipTypeName = getNameFromType(owner.type);
        ownershipTypeDescription = getDescriptionFromType(owner.type);
    }

    const avatar: React.ReactNode = (
        <CustomAvatar
            name={name}
            photoUrl={pictureLink}
            useDefaultAvatar={false}
            hideTooltip
            style={{ marginRight: 0 }}
        />
    );

    /* TODO: We probably do not want render if ownership type has been soft deleted */

    return (
        <ContentWrapper>
            <AvatarWrapper>{avatar}</AvatarWrapper>
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
