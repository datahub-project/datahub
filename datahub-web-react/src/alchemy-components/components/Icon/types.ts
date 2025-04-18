import { HTMLAttributes } from 'react';

import type { FontSizeOptions, FontColorOptions, RotationOptions } from '@components/theme/config';
import { AVAILABLE_ICONS } from './constants';

// Utility function to create an enum from an array of strings
function createEnum<T extends string>(values: T[]): { [K in T]: K } {
    return values.reduce((acc, value) => {
        acc[value] = value;
        return acc;
    }, Object.create(null));
}

const names = createEnum(AVAILABLE_ICONS);
export type IconNames = keyof typeof names;

export type MaterialIconVariant = 'filled' | 'outline';
export type IconSource = 'material' | 'phosphor';

export interface IconPropsDefaults {
    source: IconSource;
    variant: MaterialIconVariant;
    size: FontSizeOptions;
    color: FontColorOptions;
    rotate: RotationOptions;
}

export interface IconProps extends Partial<IconPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    icon: IconNames;
    className?: string;
}
