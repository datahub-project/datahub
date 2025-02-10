import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const Text = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

export const EmptyDomainsYouOwn = () => {
    return (
        <Text>
            You are not the owner of any domains yet.
            <br />
            <a target="_blank" rel="noreferrer noopener" href="https://datahubproject.io/docs/domains">
                Learn more
            </a>{' '}
            about domains.
        </Text>
    );
};
