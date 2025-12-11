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

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const Header = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    padding-right: 40px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

const Body = styled.div``;

const HeaderSkeleton = styled(Skeleton.Input)`
    && {
        padding: 0px 12px 12px 0px;
        height: 60px;
        border-radius: 8px;
        width: 540px;
    }
`;

const CardWrapper = styled.div`
    padding: 20px 40px;
`;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 0px 12px 12px 0px;
        height: 60px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const TableLoadingSkeleton = () => {
    return (
        <>
            <Header>
                <HeaderSkeleton active size="large" />
            </Header>
            <Body>
                <CardWrapper>
                    <CardSkeleton active size="large" />
                </CardWrapper>
                <CardWrapper>
                    <CardSkeleton active size="large" />
                </CardWrapper>
                <CardWrapper>
                    <CardSkeleton active size="large" />
                </CardWrapper>
            </Body>
        </>
    );
};
