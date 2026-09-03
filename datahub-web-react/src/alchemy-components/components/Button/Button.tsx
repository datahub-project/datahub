import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import React from 'react';
import styled, { keyframes } from 'styled-components';

import { ButtonBase } from '@components/components/Button/components';
import { ButtonProps, ButtonPropsDefaults } from '@components/components/Button/types';
import { Icon } from '@components/components/Icon';

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

const LoadingSpinner = styled(CircleNotch)`
    animation: ${spin} 1s linear infinite;
    color: currentColor;
`;

export const buttonDefaults: ButtonPropsDefaults = {
    variant: 'filled',
    color: 'primary',
    size: 'md',
    iconPosition: 'left',
    isCircle: false,
    isLoading: false,
    isActive: false,
};

export const Button = ({
    variant = buttonDefaults.variant,
    color = buttonDefaults.color,
    size = buttonDefaults.size,
    icon, // default undefined
    iconPosition = buttonDefaults.iconPosition,
    isCircle = buttonDefaults.isCircle,
    isLoading = buttonDefaults.isLoading,
    isActive = buttonDefaults.isActive,
    children,
    ...props
}: ButtonProps) => {
    const styleProps = {
        variant,
        color,
        size,
        isCircle,
        isLoading,
        isActive,
        hasChildren: !!children,
    };

    if (isLoading) {
        return (
            <ButtonBase {...styleProps} {...props}>
                <LoadingSpinner /> {!isCircle && children}
            </ButtonBase>
        );
    }

    // Prefer `icon.size` over `size` for icon size
    return (
        <ButtonBase {...styleProps} {...props}>
            {icon && iconPosition === 'left' && <Icon size={size} {...icon} />}
            {!isCircle && children}
            {icon && iconPosition === 'right' && <Icon size={size} {...icon} />}
        </ButtonBase>
    );
};
