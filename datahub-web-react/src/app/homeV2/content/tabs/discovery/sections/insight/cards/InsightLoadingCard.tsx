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

const Container = styled.div``;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        width: 340px;
        height: 270px;
        border-radius: 10px;
    }
`;

export const InsightLoadingCard = () => {
    return (
        <Container>
            <CardSkeleton active size="large" />
        </Container>
    );
};
