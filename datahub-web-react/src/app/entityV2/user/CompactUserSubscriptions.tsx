import React, { useState } from 'react';
import { Container, SubscriptionContainer } from '../shared/SidebarStyledComponents';
import { EntityLink } from '../../homeV2/reference/sections/EntityLink';
import { GenericEntityProperties } from '../../entity/shared/types';
import { ShowMoreSection } from '../shared/sidebarSection/ShowMoreSection';
import { DataHubSubscription } from '../../../types.generated';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

type Props = {
    subscriptions: DataHubSubscription[];
};

export const CompactUserSubscriptions = ({ subscriptions }: Props) => {
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    const subscriptionCount = subscriptions?.length || 0;

    return (
        <>
            <SubscriptionContainer>
                {subscriptions.map((subscription, index) => {
                    const { entity } = subscription;
                    return (
                        index < entityCount && (
                            <Container>
                                <EntityLink key={`${entity.urn}`} entity={entity as GenericEntityProperties} />
                            </Container>
                        )
                    );
                })}
            </SubscriptionContainer>
            {subscriptionCount > entityCount && (
                <ShowMoreSection
                    totalCount={subscriptionCount}
                    entityCount={entityCount}
                    setEntityCount={setEntityCount}
                    showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                />
            )}
        </>
    );
};
