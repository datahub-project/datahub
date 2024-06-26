import { ReactElement, ButtonHTMLAttributes } from 'react';
import type { SizeOptions, ColorOptions } from '../theme/config';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
    variant?: 'filled' | 'outline' | 'text';
    // corresponds to the "colorStates" in the design system
    color?: ColorOptions;
    size?: SizeOptions;
    isCircle?: boolean;
    icon?: ReactElement;
    iconPosition?: 'left' | 'right';
    isLoading?: boolean;
}

export const buttonDefaults = {
    variant: 'filled' as ButtonProps['variant'],
    color: 'violet' as ButtonProps['color'],
    size: 'md' as ButtonProps['size'],
    isCircle: false,
    iconPosition: 'left' as ButtonProps['iconPosition'],
    isLoading: false,
};
