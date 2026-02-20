import { Avatar, Icon, colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType, OwnerType } from '@types';

const ContentWrapper = styled.div<{ $isProposed?: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 6px 3px 4px;
    border-radius: 20px;
    border: 1px ${({ $isProposed }) => ($isProposed ? 'dashed' : 'solid')} ${colors.gray[100]};

    :hover {
        cursor: pointer;
    }
`;

const NameText = styled.span`
    color: ${colors.gray[1700]};
    font-weight: 600;
    font-size: 12px;
`;

const StyledIcon = styled(Icon)`
    margin-left: 4px;
`;

interface Props {
    actor?: OwnerType;
    isProposed?: boolean;
    onClose?: (e: any) => void;
    hideLink?: boolean;
    propagationDetails?: AttributionDetails;
}

export default function ActorPill({ actor, isProposed, onClose, hideLink, propagationDetails }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const name = actor && entityRegistry.getDisplayName(actor.type, actor);
    const avatarUrl = actor?.editableProperties?.pictureLink || undefined;
    const linkProps = useEmbeddedProfileLinkProps();

    if (!actor) return null;

    const avatarType = actor.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

    return (
        <HoverEntityTooltip entity={actor} showArrow={false} previewContext={{ propagationDetails }}>
            <Link
                to={hideLink ? null : `${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}
                data-testid={`owner-${actor.urn}`}
                {...linkProps}
            >
                <ContentWrapper $isProposed={isProposed} data-testid={`${isProposed ? 'proposed-' : ''}owner-${name}`}>
                    <Avatar name={name || ''} imageUrl={avatarUrl} type={avatarType} />
                    <NameText data-testid="owner-name">{name}</NameText>
                    {!isProposed && onClose && (
                        <StyledIcon
                            onClick={onClose}
                            icon="X"
                            source="phosphor"
                            size="sm"
                            color="gray"
                            data-testid={`remove-owner-${actor.urn}`}
                        />
                    )}
                </ContentWrapper>
            </Link>
        </HoverEntityTooltip>
    );
}
