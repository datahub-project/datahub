import { LoadingOutlined } from '@ant-design/icons';
import { Icon } from '@components';
import React from 'react';

import { ButtonBase } from '@components/components/Button/components';
import { ButtonProps, ButtonPropsDefaults } from '@components/components/Button/types';

export const buttonDefaults: ButtonPropsDefaults = {
    variant: 'filled',
    color: 'primary',
    size: 'md',
    iconPosition: 'left',
    isCircle: false,
    isLoading: false,
    isDisabled: false,
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
    isDisabled = buttonDefaults.isDisabled,
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
        isDisabled,
        hasChildren: !!children,
    };

    if (isLoading) {
        return (
            <ButtonBase {...styleProps} {...props}>
                <LoadingOutlined rotate={10} /> {!isCircle && children}
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
