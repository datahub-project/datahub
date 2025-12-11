/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const Text = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

export const EmptyDomainsYouOwn = () => {
    return (
        <Text>
            You are not the owner of any domains yet.
            <br />
            <a target="_blank" rel="noreferrer noopener" href="https://docs.datahub.com/docs/domains">
                Learn more
            </a>{' '}
            about domains.
        </Text>
    );
};
