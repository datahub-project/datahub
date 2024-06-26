import React from 'react';

import { LoadingOutlined } from '@ant-design/icons';

import { ButtonBase } from './components';
import { buttonDefaults, ButtonProps } from './types';

export const Button = ({
    variant = buttonDefaults.variant,
    color = buttonDefaults.color,
    size = buttonDefaults.size,
    isCircle = buttonDefaults.isCircle,
    icon, // default undefined
    iconPosition = buttonDefaults.iconPosition,
    isLoading = buttonDefaults.isLoading,
    children,
    ...props
}: ButtonProps) => {
    if (isLoading) {
        return (
            <ButtonBase
                variant={variant}
                color={color}
                size={size}
                isCircle={isCircle}
                isLoading={isLoading}
                disabled={isLoading || props.disabled}
                {...props}
            >
                <LoadingOutlined rotate={10} /> {!isCircle && children}
            </ButtonBase>
        );
    }

    return (
        <ButtonBase
            variant={variant}
            color={color}
            size={size}
            isCircle={isCircle}
            isLoading={isLoading}
            disabled={isLoading || props.disabled}
            {...props}
        >
            {icon && iconPosition === 'left' && icon}
            {!isCircle && children}
            {icon && iconPosition === 'right' && icon}
        </ButtonBase>
    );
};
