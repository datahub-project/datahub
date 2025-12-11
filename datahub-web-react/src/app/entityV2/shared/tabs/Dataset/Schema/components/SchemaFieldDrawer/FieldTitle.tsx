/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const FieldName = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.WHITE_WIRE};
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
    overflow: hidden;
    display: block;
    cursor: pointer;
    :hover {
        font-weight: bold;
    }
`;

interface Props {
    displayName: string;
}

export default function FieldTitle({ displayName }: Props) {
    const name = displayName.split('.').pop();
    return <FieldName>{name}</FieldName>;
}
