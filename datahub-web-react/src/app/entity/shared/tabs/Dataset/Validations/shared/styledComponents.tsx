/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, ClockCircleOutlined, CloseOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import {
    FAILURE_COLOR_HEX,
    SUCCESS_COLOR_HEX,
    WARNING_COLOR_HEX,
} from '@app/entity/shared/tabs/Dataset/Validations/utils';

export const StyledCheckOutlined = styled(CheckOutlined)`
    color: ${SUCCESS_COLOR_HEX};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledCloseOutlined = styled(CloseOutlined)`
    color: ${FAILURE_COLOR_HEX};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledExclamationOutlined = styled(ExclamationCircleOutlined)`
    color: ${WARNING_COLOR_HEX};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    color: ${ANTD_GRAY[6]};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;
