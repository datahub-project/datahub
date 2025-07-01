import { ButtonHTMLAttributes } from 'react';

import { IconProps } from '@components/components/Icon/types';
import type { ColorOptions, SizeOptions } from '@components/theme/config';

import { Theme } from '@src/conf/theme/types';

export enum ButtonVariantValues {
    filled = 'filled',
    outline = 'outline',
    text = 'text',
    secondary = 'secondary',
}
export type ButtonVariant = keyof typeof ButtonVariantValues;

export interface ButtonPropsDefaults {
    variant: ButtonVariant;
    color: ColorOptions;
    size: SizeOptions;
    iconPosition: 'left' | 'right';
    isCircle: boolean;
    isLoading: boolean;
    isDisabled: boolean;
    isActive: boolean;
}

export interface ButtonProps
    extends Partial<ButtonPropsDefaults>,
        Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'color'> {
    icon?: IconProps;
}

export type ButtonStyleProps = Omit<ButtonPropsDefaults, 'iconPosition'> & { hasChildren: boolean; theme?: Theme };
