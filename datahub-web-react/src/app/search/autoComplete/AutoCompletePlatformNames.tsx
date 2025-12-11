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

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 500;
    color: ${ANTD_GRAY_V2[8]};
    white-space: nowrap;
`;

type Props = {
    platforms: Array<string>;
};

const AutoCompletePlatformNames = ({ platforms }: Props) => {
    return <PlatformText>{platforms.join(' & ')}</PlatformText>;
};

export default AutoCompletePlatformNames;
