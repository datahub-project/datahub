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

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    padding: 8px;
    gap: 16px;
    width: 100%;
`;

const SkeletonRow = styled(Skeleton.Input)`
    && {
        width: 100%;
    }
`;

export default function TreeViewSkeleton() {
    return (
        <Wrapper>
            <SkeletonRow active size="small" />
            <SkeletonRow active size="small" />
            <SkeletonRow active size="small" />
        </Wrapper>
    );
}
