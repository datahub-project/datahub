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

const ContextSkeleton = styled(Skeleton.Input)`
    && {
        width: 320px;
        border-radius: 4px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

export default function EntityPlatformLoadingSection() {
    return (
        <Space direction="vertical">
            <ContextSkeleton active size="small" />
        </Space>
    );
}
