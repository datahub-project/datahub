import { ButtonHTMLAttributes } from 'react';

import type { IconNames } from '@components';
import type { SizeOptions, ColorOptions, FontSizeOptions } from '@components/theme/config';
import { IconSource } from '../Icon/types';

export type ButtonVariant = 'filled' | 'outline' | 'text';

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
    icon?: IconNames;
    iconSize?: FontSizeOptions;
    iconSource?: IconSource;
    iconColor?: ColorOptions;
}

export type ButtonStyleProps = Omit<ButtonPropsDefaults, 'iconPosition'>;
