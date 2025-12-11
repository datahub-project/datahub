/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
    margin-left: 12px 0px 12px 0px;
`;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 2px 12px 2px 0px;
        height: 20px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const AssertionSummaryLoading = () => {
    return (
        <Container>
            <CardSkeleton active size="large" />
            <CardSkeleton active size="large" />
            <CardSkeleton active size="large" />
        </Container>
    );
};
