import { Button, Icon } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled from 'styled-components';

export const SidebarWrapper = styled.div<{ width: number; $isShowNavBarRedesign?: boolean }>`
    max-height: 100%;
    width: ${(props) => props.width}px;
    min-width: ${(props) => props.width}px;
    display: ${(props) => (props.width ? 'block' : 'none')};
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) => !props.$isShowNavBarRedesign && 'margin-bottom: 12px;'}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        box-shadow: ${props.theme.colors.shadowSm};
    `}
`;

const StyledButton = styled(Button)`
    padding: 0;
    height: 16px;
    width: 16px;
`;

const StyledIcon = styled(Icon)<{ $isOpen?: boolean }>`
    color: ${(props) => props.theme.colors.icon};

    transform: rotate(${(props) => (props.$isOpen ? '90' : '0')}deg);
    transition: transform 250ms;
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
        <StyledButton onClick={onClick} data-testid={dataTestId} variant="text">
            <StyledIcon icon={CaretRight} $isOpen={isOpen} size="md" />
        </StyledButton>
    );
}
