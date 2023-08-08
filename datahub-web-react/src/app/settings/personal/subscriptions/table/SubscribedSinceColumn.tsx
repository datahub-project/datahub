import React from 'react';
import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { DataHubSubscription } from '../../../../../types.generated';

dayjs.extend(LocalizedFormat);

const SubscribedSinceText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
`;

interface Props {
    subscription: DataHubSubscription;
}

export function SubscribedSinceColumn({ subscription }: Props) {
    const subscribedSinceDate = dayjs(subscription.createdOn.time).format('ll');

    return <SubscribedSinceText>{subscribedSinceDate}</SubscribedSinceText>;
}
