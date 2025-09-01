import { Icon, Text, colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { CustomAvatar } from '@app/shared/avatar';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { OwnerType } from '@types';

const ContentWrapper = styled.div<{ $isProposed?: boolean }>`
    display: inline-flex;
    align-items: center;
    padding: 2px 6px 2px 4px;
    border-radius: 20px;
    border: 1px ${({ $isProposed }) => ($isProposed ? 'dashed' : 'solid')} ${colors.gray[1400]};

    :hover {
        cursor: pointer;
    }
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

    return (
        <HoverEntityTooltip entity={actor} showArrow={false} previewContext={{ propagationDetails }}>
            <Link to={hideLink ? null : `${entityRegistry.getEntityUrl(actor.type, actor.urn)}`} {...linkProps}>
                <ContentWrapper $isProposed={isProposed}>
                    <CustomAvatar size={20} name={name} photoUrl={avatarUrl} hideTooltip />
                    <Text color="gray" size="sm">
                        {name}
                    </Text>
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
