/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from 'antd';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

export const SeeMore = styled(Button)`
    margin-top: -20px;
    background-color: ${ANTD_GRAY[4]};
    padding: 8px;
    border: none;
    line-height: 8px;
    height: 20px;
`;
