/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Card } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Body = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    padding: 24px;
    row-gap: 24px;
    column-gap: 24px;
    overflow: auto;
`;

const CardSkeleton = styled(Card)`
    && {
        padding: 0px 12px 12px 0px;
        height: 210px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const AcrylAssertionsSummaryTabLoading = () => {
    return (
        <Body>
            <CardSkeleton loading />
            <CardSkeleton loading />
            <CardSkeleton loading />
            <CardSkeleton loading />
        </Body>
    );
};
