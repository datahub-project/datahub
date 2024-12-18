import React from 'react';

import { LoadingOutlined } from '@ant-design/icons';

import { Icon } from '@components';

import { ButtonBase } from './components';
import { ButtonProps } from './types';

export const buttonDefaults: ButtonProps = {
    variant: 'filled',
    color: 'violet',
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
    const sharedProps = {
        variant,
        color,
        size,
        isCircle,
        isLoading,
        isActive,
        isDisabled,
        disabled: isDisabled,
    };

    if (isLoading) {
        return (
            <ButtonBase {...sharedProps} {...props}>
                <LoadingOutlined rotate={10} /> {!isCircle && children}
            </ButtonBase>
        );
    }

    return (
        <ButtonBase {...sharedProps} {...props}>
            {icon && iconPosition === 'left' && <Icon icon={icon} size={size} />}
            {!isCircle && children}
            {icon && iconPosition === 'right' && <Icon icon={icon} size={size} />}
        </ButtonBase>
    );
};
