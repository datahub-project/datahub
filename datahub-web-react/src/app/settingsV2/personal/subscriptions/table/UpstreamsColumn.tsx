import { CheckCircleFilled, StopOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';

import { DataHubSubscription, SubscriptionType } from '@types';

const UpstreamsColumnContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    gap: 8px;
`;

interface Props {
    subscription: DataHubSubscription;
}

export function UpstreamsColumn({ subscription }: Props) {
    const isSubscribedToUpstreams: boolean = subscription.subscriptionTypes.includes(
        SubscriptionType.UpstreamEntityChange,
    );

    return (
        <UpstreamsColumnContainer>
            {isSubscribedToUpstreams ? <CheckCircleFilled /> : <StopOutlined />}
        </UpstreamsColumnContainer>
    );
}
