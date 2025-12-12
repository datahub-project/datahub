import React from 'react';
import styled, { css } from 'styled-components';

import { Button } from '@components/components/Button';
import { Card } from '@components/components/Card';
import { Text } from '@components/components/Text';

import { colors } from '@src/alchemy-components/theme';

export type CalloutPosition = 'inline' | 'fixed-top-right' | 'fixed-bottom-center';

const getPositionStyles = (position: CalloutPosition) => {
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

const CalloutContainer = styled.div<{ $position: CalloutPosition }>`
    ${({ $position }) => getPositionStyles($position)}
`;

const CardWrapper = styled.div<{ $isInline: boolean; $size: 'sm' | 'md' }>`
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

const Content = styled.div`
    color: ${colors.gray[1700]};
    line-height: 1.5;
`;

const Footer = styled.div`
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 8px;
    margin-top: 16px;
`;

export interface CalloutCardProps {
    /** Icon element to display */
    icon: React.ReactNode;
    /** Title text */
    title: string;
    /** Content - can be string or React node */
    children: React.ReactNode;
    /** Position of the callout card */
    position?: CalloutPosition;
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
 *
 * Unlike Popover (which requires a trigger element), CalloutCard is standalone
 * and can be positioned at fixed screen locations.
 */
export const CalloutCard = ({
    icon,
    title,
    children,
    position = 'inline',
    size = 'sm',
    primaryButtonText,
    onPrimaryClick,
    showCloseButton = false,
    onClose,
}: CalloutCardProps) => {
    return (
        <CalloutContainer $position={position}>
            <CardWrapper $isInline={position === 'inline'} $size={size}>
                <Card
                    title={
                        <Text size={size === 'sm' ? 'md' : 'lg'} weight="bold" color="gray" colorLevel={600}>
                            {title}
                        </Text>
                    }
                    icon={icon}
                    iconAlignment="horizontal"
                    isCardClickable={false}
                    width="100%"
                >
                    <Content>{children}</Content>
                    <Footer>
                        {showCloseButton && onClose && (
                            <Button variant="text" onClick={onClose}>
                                Close
                            </Button>
                        )}
                        <Button variant="filled" onClick={onPrimaryClick}>
                            {primaryButtonText}
                        </Button>
                    </Footer>
                </Card>
            </CardWrapper>
        </CalloutContainer>
    );
};
