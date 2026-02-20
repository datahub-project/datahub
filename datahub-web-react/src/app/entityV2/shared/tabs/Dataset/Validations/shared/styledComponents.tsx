import { CheckOutlined, ClockCircleOutlined, CloseOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';

export const StyledCheckOutlined = styled(CheckOutlined)`
    color: ${(props) => props.theme.colors.iconSuccess};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledCloseOutlined = styled(CloseOutlined)`
    color: ${(props) => props.theme.colors.iconError};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledExclamationOutlined = styled(ExclamationCircleOutlined)`
    color: ${(props) => props.theme.colors.iconWarning};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    color: ${(props) => props.theme.colors.iconDisabled};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;
