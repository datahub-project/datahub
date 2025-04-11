import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const Text = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

export const EmptyAssetsYouOwn = () => {
    return (
        <Text>
            You do not own any assets yet.
            <br />
            <a
                target="_blank"
                rel="noreferrer noopener"
                href="https://datahubproject.io/docs/ownership/ownership-types"
            >
                Learn more
            </a>{' '}
            about ownership.
        </Text>
    );
};
