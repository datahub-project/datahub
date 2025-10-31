import { Tooltip } from '@components';
import { Typography } from 'antd';
import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import React from 'react';
import styled from 'styled-components/macro';

import { getTimeFromNow } from '@app/shared/time/timeUtils';

import { DataHubSubscription } from '@types';

dayjs.extend(LocalizedFormat);

const SubscribedSinceText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
`;

const DEFAULT_DATETIME_FORMAT = 'l @ LT (z)';

interface Props {
    subscription: DataHubSubscription;
}

const SubscriptionsSubscribedSinceColumn = ({ subscription }: Props) => {
    const subscribedSinceDate = getTimeFromNow(subscription.createdOn.time);

    return (
        <Tooltip placement="top" title={dayjs(subscription.createdOn.time).format(DEFAULT_DATETIME_FORMAT)}>
            <SubscribedSinceText>{subscribedSinceDate}</SubscribedSinceText>
        </Tooltip>
    );
};

export default SubscriptionsSubscribedSinceColumn;
