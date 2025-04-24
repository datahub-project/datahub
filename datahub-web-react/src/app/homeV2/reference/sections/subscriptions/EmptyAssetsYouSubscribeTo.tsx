import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const Text = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

export const EmptyAssetsYouSubscribeTo = () => {
    return (
        <Text>
            You have not subscribed to any assets yet. <br />
            <a
                target="_blank"
                rel="noreferrer noopener"
                href="https://datahubproject.io/docs/next/managed-datahub/subscription-and-notification/"
            >
                Learn more
            </a>{' '}
            about subscriptions.
        </Text>
    );
};
