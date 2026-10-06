import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { X } from '@phosphor-icons/react/dist/csr/X';
import styled from 'styled-components';

export const StyledCheckOutlined = styled(Check)`
    color: ${(props) => props.theme.colors.iconSuccess};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledCloseOutlined = styled(X)`
    color: ${(props) => props.theme.colors.iconError};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledExclamationOutlined = styled(WarningCircle)`
    color: ${(props) => props.theme.colors.iconWarning};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;

export const StyledClockCircleOutlined = styled(Clock)`
    color: ${(props) => props.theme.colors.iconDisabled};
    font-size: 16px;
    margin-right: 4px;
    margin-left: 4px;
`;
