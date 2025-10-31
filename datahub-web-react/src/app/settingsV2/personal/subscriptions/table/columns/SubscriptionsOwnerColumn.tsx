import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';

import analytics, { EventType } from '@app/analytics';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubSubscription } from '@types';

const OwnerContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

interface Props {
    subscription: DataHubSubscription;
}

const SubscriptionsOwnerColumn = ({ subscription }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const { actor } = subscription;
    return (
        <OwnerContainer>
            <Link
                to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}
                onClick={(e) => {
                    analytics.event({
                        type: EventType.SubscriptionOwnerClickEvent,
                        subscriptionUrn: subscription.subscriptionUrn,
                        ownerUrn: actor.urn,
                    });
                    e.stopPropagation();
                }}
            >
                <Avatar
                    name={entityRegistry.getDisplayName(actor.type, actor)}
                    imageUrl={actor.editableProperties?.pictureLink}
                    showInPill
                    type={mapEntityTypeToAvatarType(actor.type)}
                />
            </Link>
        </OwnerContainer>
    );
};

export default SubscriptionsOwnerColumn;
