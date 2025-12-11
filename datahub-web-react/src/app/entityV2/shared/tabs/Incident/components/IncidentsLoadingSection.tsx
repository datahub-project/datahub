/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider, Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 12px;
    margin-top: 20px;
`;

const CardWrapper = styled.div`
    padding: 0px 40px;
`;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 0px 20px 20px 0px;
        height: 100px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const IncidentsLoadingSection = () => {
    return (
        <Container>
            <CardWrapper>
                <CardSkeleton active size="large" />
            </CardWrapper>
            <Divider />
            <CardWrapper>
                <CardSkeleton active size="large" />
            </CardWrapper>
            <Divider />
            <CardWrapper>
                <CardSkeleton active size="large" />
            </CardWrapper>
            <Divider />
        </Container>
    );
};
