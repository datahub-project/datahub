import { Icon } from '@components';
import { CircleNotch } from '@phosphor-icons/react';
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
    isActive: false,
};

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
    (
        {
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
        },
        ref,
    ) => {
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
                <ButtonBase ref={ref} {...styleProps} {...props}>
                    <CircleNotch className="anticon-spin" /> {!isCircle && children}
                </ButtonBase>
            );
        }

        const iconSize = ({ xs: 'sm', sm: 'md', md: 'lg', lg: 'xl', xl: '2xl' } as const)[size] || size;

        return (
            <ButtonBase ref={ref} {...styleProps} {...props}>
                {icon && iconPosition === 'left' && <Icon size={iconSize} {...icon} />}
                {!isCircle && children}
                {icon && iconPosition === 'right' && <Icon size={iconSize} {...icon} />}
            </ButtonBase>
        );
    },
);

Button.displayName = 'Button';
