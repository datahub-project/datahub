import { Avatar, Icon } from '@components';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { LinkWrapper } from '@app/sharedV2/owners/components/LinkWrapper';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { OwnerType } from '@types';

const AvatarWrapper = styled.div``;

interface Props {
    owner: OwnerType | undefined;
    onRemove?: (e: React.MouseEvent<HTMLElement>) => void;
    readonly?: boolean;
    isProposed?: boolean;
    hideLink?: boolean;
    hidePopOver?: boolean;
    propagationDetails?: AttributionDetails;
}

export default function OwnerPill({
    owner,
    onRemove,
    readonly,
    isProposed,
    hideLink,
    hidePopOver,
    propagationDetails,
}: Props) {
    const entityRegistry = useEntityRegistryV2();

    const removable = useMemo(() => !readonly && !!onRemove && !isProposed, [readonly, onRemove, isProposed]);

    const onRemoveClick = useCallback(
        (e: React.MouseEvent<HTMLElement>) => {
            e.stopPropagation();
            onRemove?.(e);
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
                        data-testid="button-remove"
                    />
                ) : null}
            </>
        );
    }, [removable, onRemoveClick]);

    if (!owner) return null;

    const avatarPhotoUrl = owner.editableProperties?.pictureLink;
    const avatarType = mapEntityTypeToAvatarType(owner.type);
    const userName = entityRegistry.getDisplayName(owner.type, owner);

    return (
        <HoverEntityTooltip
            entity={owner}
            showArrow={false}
            canOpen={!hidePopOver}
            previewContext={{ propagationDetails }}
        >
            <AvatarWrapper>
                <LinkWrapper url={entityRegistry.getEntityUrl(owner.type, owner.urn)} hide={hideLink}>
                    <Avatar
                        name={userName}
                        size="xs"
                        imageUrl={avatarPhotoUrl}
                        showInPill
                        extraRightContent={renderButtons()}
                        type={avatarType}
                        pillBorderType={isProposed ? 'dashed' : 'default'}
                        dataTestId={`owner-pill-${owner.urn}`}
                    />
                </LinkWrapper>
            </AvatarWrapper>
        </HoverEntityTooltip>
    );
}
