import { ButtonHTMLAttributes } from 'react';

import type { IconNames } from '../Icon/types';
import type { SizeOptions, ColorOptions } from '../theme/config';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
    variant?: 'filled' | 'outline' | 'text';
    color?: ColorOptions;
    size?: SizeOptions;
    icon?: IconNames;
    iconPosition?: 'left' | 'right';
    isCircle?: boolean;
    isLoading?: boolean;
    isDisabled?: boolean;
    isActive?: boolean;
}
