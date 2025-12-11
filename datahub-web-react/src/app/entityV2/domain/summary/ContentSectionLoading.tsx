/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Skeleton, Space } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const TypeSkeleton = styled(Skeleton.Input)`
    && {
        width: 60px;
        height: 60px;
        border-radius: 8px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

export default function ContentSectionLoading() {
    return (
        <Space direction="horizontal">
            <TypeSkeleton active size="default" />
            <TypeSkeleton active size="default" />
            <TypeSkeleton active size="default" />
            <TypeSkeleton active size="default" />
        </Space>
    );
}
