import styled from 'styled-components';
import { CheckOutlined, ClockCircleOutlined, CloseOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../constants';
import { FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX, WARNING_COLOR_HEX } from '../acrylUtils';

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
