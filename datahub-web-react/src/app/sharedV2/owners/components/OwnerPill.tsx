import { Avatar, Icon, Text } from '@components';
import { PopoverProps } from 'antd';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';

import ProposedIcon from '@app/entityV2/shared/sidebarSection/ProposedIcon';
import { PartialExtendedOwner } from '@app/sharedV2/owners/types';
import { getOwnershipTypeDescriptionFromOwner, getOwnershipTypeNameFromOwner } from '@app/sharedV2/owners/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

const NamePopoverTitleContainer = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const NamePopoverDescriptionContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    owner: PartialExtendedOwner;
    onRemove?: () => void;
    readonly?: boolean;
    hideNamePopover?: boolean;
}

export default function OwnerPill({ owner, onRemove, readonly, hideNamePopover }: Props) {
    const entityRegistry = useEntityRegistryV2();

    const avatarPhotoUrl = owner?.owner?.editableProperties?.pictureLink;
    const userName = entityRegistry.getDisplayName(owner.owner.type, owner.owner);

    const isProposed = !!owner.isProposed;
    const removable = !readonly && !isProposed;

    const onRemoveClick = useCallback(
        (e: React.MouseEvent<HTMLElement>) => {
            e.stopPropagation();
            onRemove?.();
        },
        [onRemove],
    );

    const renderButtons = useCallback(() => {
        return (
            <>
                {isProposed ? <ProposedIcon propertyName="Owner" /> : null}
                {removable ? (
                    <Icon
                        icon="XCircle"
                        source="phosphor"
                        color="gray"
                        size="lg"
                        colorLevel={400}
                        onClick={onRemoveClick}
                    />
                ) : null}
            </>
        );
    }, [isProposed, removable, onRemoveClick]);

    const avatarType = mapEntityTypeToAvatarType(owner.owner.type);

    const namePopoverProps: PopoverProps | undefined = useMemo(() => {
        if (hideNamePopover) return undefined;

        const ownerEntityTypeDisplayName = entityRegistry.getEntityName(owner.owner.type);
        const ownershipTypeName = getOwnershipTypeNameFromOwner(owner);
        const ownershipTypeDescription = getOwnershipTypeDescriptionFromOwner(owner);

        // do not show the name popover if owner don't have ownership type
        if (!ownershipTypeName) return undefined;

        return {
            overlayStyle: { maxWidth: 200 },
            title: (
                <NamePopoverTitleContainer>
                    <Avatar name={userName} size="lg" imageUrl={avatarPhotoUrl} type={avatarType} />{' '}
                    <Text weight="semiBold">{ownerEntityTypeDisplayName}</Text>{' '}
                </NamePopoverTitleContainer>
            ),
            content: (
                <NamePopoverDescriptionContainer>
                    <Text weight="semiBold" size="sm">
                        {userName}
                    </Text>
                    <Text weight="semiBold" size="xs">
                        {ownershipTypeName}
                    </Text>
                    <Text size="xs">{ownershipTypeDescription}</Text>
                </NamePopoverDescriptionContainer>
            ),
        };
    }, [hideNamePopover, avatarType, userName, avatarPhotoUrl, owner, entityRegistry]);

    return (
        <Avatar
            name={userName}
            size="xs"
            imageUrl={avatarPhotoUrl}
            showInPill
            extraRightContent={renderButtons()}
            pillBorderType={isProposed ? 'dashed' : 'default'}
            type={avatarType}
            namePopover={namePopoverProps}
        />
    );
}
