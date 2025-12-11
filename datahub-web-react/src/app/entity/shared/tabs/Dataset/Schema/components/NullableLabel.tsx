/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { blue } from '@ant-design/colors';
import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const NullableBadge = styled(Badge)`
    margin-left: 4px;
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY[1]};
        color: ${blue[5]};
        border: 1px solid ${blue[3]};
        font-size: 12px;
        font-weight: 400;
        height: 22px;
    }
`;

export default function NullableLabel() {
    return <NullableBadge count="Nullable" />;
}
