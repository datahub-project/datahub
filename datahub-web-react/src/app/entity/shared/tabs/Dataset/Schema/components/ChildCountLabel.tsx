/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';

type Props = {
    count: number;
};

const ChildCountBadge = styled(Badge)`
    margin-left: 10px;
    margin-top: 16px;
    margin-bottom: 16px;
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY_V2[1]};
        color: ${ANTD_GRAY_V2[8]};
        box-shadow: 0 2px 1px -1px ${ANTD_GRAY_V2[6]};
        border-radius: 4px 4px 4px 4px;
        font-size: 12px;
        font-weight: 500;
        height: 22px;
        font-family: 'Manrope';
    }
`;

export default function ChildCountLabel({ count }: Props) {
    const propertyString = count > 1 ? ' properties' : ' property';

    // eslint-disable-next-line
    return <ChildCountBadge count={count.toString() + propertyString} />;
}
