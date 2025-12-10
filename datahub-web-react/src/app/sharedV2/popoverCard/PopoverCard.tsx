import { Button, Icon, Text, colors, typography } from '@components';
import React from 'react';
import styled, { css } from 'styled-components';

export type PopoverPosition = 'inline' | 'fixed-top-right' | 'fixed-bottom-center';

const getPositionStyles = (position: PopoverPosition) => {
    switch (position) {
        case 'inline':
            return css`
                position: relative;
                height: 0;
                display: flex;
                justify-content: center;
            `;
        case 'fixed-top-right':
            return css`
                position: fixed;
                top: 80px;
                right: 40px;
                z-index: 1000;
            `;
        case 'fixed-bottom-center':
            return css`
                position: fixed;
                bottom: 40px;
                left: 50%;
                transform: translateX(-50%);
                z-index: 1000;
            `;
        default:
            return '';
    }
};

const PopoverContainer = styled.div<{ $position: PopoverPosition }>`
    ${({ $position }) => getPositionStyles($position)}
`;

const Card = styled.div<{ $isInline: boolean; $size: 'sm' | 'md' }>`
    background: ${colors.white};
    border-radius: 12px;
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.15);
    padding: ${({ $size }) => ($size === 'sm' ? '16px 20px' : '20px 24px')};
    min-width: ${({ $size }) => ($size === 'sm' ? '300px' : '400px')};
    max-width: ${({ $size }) => ($size === 'sm' ? '400px' : '500px')};

    ${({ $isInline }) =>
        $isInline &&
        css`
            position: absolute;
            top: 16px;
            z-index: 100;
        `}
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
`;

const Title = styled(Text)<{ $size: 'sm' | 'md' }>`
    font-size: ${({ $size }) => ($size === 'sm' ? typography.fontSizes.md : typography.fontSizes.lg)};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
`;

const Content = styled.div`
    color: ${colors.gray[1700]};
    line-height: 1.5;
    margin-bottom: 16px;
`;

const Footer = styled.div`
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 8px;
`;

const CloseButton = styled(Text)`
    color: ${colors.gray[1700]};
    cursor: pointer;
    font-size: ${typography.fontSizes.md};
    padding: 8px 12px;

    &:hover {
        color: ${colors.gray[600]};
    }
`;

export interface PopoverCardProps {
    /** Icon name from Phosphor icons */
    icon: string;
    /** Title text */
    title: string;
    /** Content - can be string or React node */
    children: React.ReactNode;
    /** Position of the popover card */
    position?: PopoverPosition;
    /** Size of the card */
    size?: 'sm' | 'md';
    /** Primary button text */
    primaryButtonText: string;
    /** Primary button click handler */
    onPrimaryClick: () => void;
    /** Show secondary close button */
    showCloseButton?: boolean;
    /** Close button click handler */
    onClose?: () => void;
}

/**
 * A floating card component that can be positioned inline or fixed on the screen.
 * Useful for guided tours, feature announcements, tips, and onboarding flows.
 */
export default function PopoverCard({
    icon,
    title,
    children,
    position = 'inline',
    size = 'sm',
    primaryButtonText,
    onPrimaryClick,
    showCloseButton = false,
    onClose,
}: PopoverCardProps) {
    return (
        <PopoverContainer $position={position}>
            <Card $isInline={position === 'inline'} $size={size}>
                <Header>
                    <Icon
                        icon={icon}
                        source="phosphor"
                        color="violet"
                        size={size === 'sm' ? 'xl' : '2xl'}
                        weight="duotone"
                    />
                    <Title $size={size}>{title}</Title>
                </Header>
                <Content>{children}</Content>
                <Footer>
                    {showCloseButton && onClose && <CloseButton onClick={onClose}>Close</CloseButton>}
                    <Button variant="filled" onClick={onPrimaryClick}>
                        {primaryButtonText}
                    </Button>
                </Footer>
            </Card>
        </PopoverContainer>
    );
}
