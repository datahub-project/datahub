import React, { HTMLAttributes } from 'react';

import styled from 'styled-components';
import { Tooltip } from '@components';

import { ANTD_GRAY } from '../../../../../../constants';

const Button = styled.div<{ disabled: boolean }>`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 16px;
    padding: 4px 12px;
    background-color: ${(props) => (props.disabled ? ANTD_GRAY[4] : props.theme.styles['primary-color'])};
    border: 1px solid ${(props) => (props.disabled ? ANTD_GRAY[4] : props.theme.styles['primary-color'])};
    color: ${(props) => (props.disabled ? ANTD_GRAY[9] : '#fff')};
    ${(props) =>
        (!props.disabled &&
            `:hover {
            color: #fff;
            opacity: 0.8;
            border-color: ${props.theme.styles['primary-color']};
            cursor: pointer;
        }
    `) ||
        null}
`;

const Icon = styled.div`
    margin-right: 8px;
`;

type Props = {
    icon?: React.ReactNode;
    title: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
} & HTMLAttributes<HTMLDivElement>;

export const PrimaryButton = ({ icon, title, tooltip, disabled = false, onClick, ...props }: Props) => {
    return (
        <Tooltip title={tooltip} placement="left" showArrow={false}>
            <Button
                disabled={disabled}
                onClick={(e) => {
                    e.stopPropagation();
                    onClick();
                }}
                {...props}
            >
                {(icon && <Icon>{icon}</Icon>) || null}
                {title}
            </Button>
        </Tooltip>
    );
};
