import { Avatar, Icon } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { PartialExtendedOwner } from '@app/sharedV2/owners/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

const AvatarWrapper = styled.div``;

interface Props {
    owner: PartialExtendedOwner;
    onRemove?: () => void;
    readonly?: boolean;
}

export default function OwnerPill({ owner, onRemove, readonly }: Props) {
    const entityRegistry = useEntityRegistryV2();

    const avatarPhotoUrl = owner?.owner?.editableProperties?.pictureLink;
    const userName = entityRegistry.getDisplayName(owner.owner.type, owner.owner);

    const removable = !readonly;

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
    }, [removable, onRemoveClick]);

    const avatarType = mapEntityTypeToAvatarType(owner.owner.type);

    return (
        <HoverEntityTooltip entity={owner.owner} showArrow={false}>
            <AvatarWrapper>
                <Avatar
                    name={userName}
                    size="xs"
                    imageUrl={avatarPhotoUrl}
                    showInPill
                    extraRightContent={renderButtons()}
                    type={avatarType}
                />
            </AvatarWrapper>
        </HoverEntityTooltip>
    );
}
