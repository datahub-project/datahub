import React from 'react';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import styled from 'styled-components';
import { RotatingButton } from '../../shared/components';

export const SidebarWrapper = styled.div<{ width: number; $isShowNavBarRedesign?: boolean }>`
    max-height: 100%;
    width: ${(props) => props.width}px;
    min-width: ${(props) => props.width}px;
    display: ${(props) => (props.width ? 'block' : 'none')};
    background-color: #fff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) => !props.$isShowNavBarRedesign && 'margin-bottom: 12px;'}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
`;

export function RotatingTriangle({
    isOpen,
    onClick,
    dataTestId,
}: {
    isOpen: boolean;
    onClick?: () => void;
    dataTestId?: string;
}) {
    return (
        <RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 90 : 0}
            icon={<ChevronRightIcon style={{ color: 'black' }} />}
            onClick={onClick}
            data-testid={dataTestId}
        />
    );
}
