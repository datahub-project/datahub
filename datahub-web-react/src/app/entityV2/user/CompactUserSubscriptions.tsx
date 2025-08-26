import { Col } from 'antd';
import React, { useState } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { SubscriptionContainer } from '@app/entityV2/shared/SidebarStyledComponents';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';

import { DataHubSubscription } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const entityLinkTextStyle = {
    overflow: 'hidden',
    'white-space': 'nowrap',
    'text-overflow': 'ellipsis',
};

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
                            <Col xs={11}>
                                <EntityLink
                                    key={`${entity.urn}`}
                                    entity={entity as GenericEntityProperties}
                                    displayTextStyle={entityLinkTextStyle}
                                />
                            </Col>
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
