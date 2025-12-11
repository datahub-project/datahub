/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Col, Row, Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const SkeletonContainer = styled(Row)`
    && {
        margin-bottom: 20px;
        border-bottom: 1px solid #ebecf0;
        .ant-skeleton-button-sm {
            height: 16px;
        }
        .ant-skeleton-avatar-circle {
            height: 16px;
            width: 16px;
        }
    }
`;
const SkeletonCol = styled(Col)`
    margin-bottom: 20px;
    width: 100%;
    display: flex;
    align-items: center;
    gap: 1rem;
`;

const SkeletonButton = styled(Skeleton.Button)<{ width?: string }>`
    &&& {
        width: ${(props) => props.width && props.width};
    }
`;

export const EntityLinkListSkeleton = () => {
    return (
        <SkeletonContainer>
            <SkeletonCol>
                <SkeletonButton active size="small" shape="square" width="10rem" />
                <Skeleton.Avatar active size="small" shape="circle" />
            </SkeletonCol>

            <SkeletonCol>
                <Skeleton.Avatar active size="small" shape="square" />
                <SkeletonButton active size="small" shape="square" block width="12rem" />
            </SkeletonCol>
            <SkeletonCol>
                <Skeleton.Avatar active size="small" shape="square" />
                <SkeletonButton active size="small" shape="square" block width="5rem" />
            </SkeletonCol>
        </SkeletonContainer>
    );
};
