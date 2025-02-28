import React, { useState } from 'react';
import { Col } from 'antd';
import { SubscriptionContainer } from '../shared/SidebarStyledComponents';
import { EntityLink } from '../../homeV2/reference/sections/EntityLink';
import { GenericEntityProperties } from '../../entity/shared/types';
import { ShowMoreSection } from '../shared/sidebarSection/ShowMoreSection';
import { DataHubSubscription } from '../../../types.generated';

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
