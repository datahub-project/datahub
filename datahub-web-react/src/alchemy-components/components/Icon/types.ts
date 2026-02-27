import { HTMLAttributes } from 'react';

import { AVAILABLE_ICONS } from '@components/components/Icon/constants';
import type {
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    RotationOptions,
} from '@components/theme/config';

function createEnum<T extends string>(values: T[]): { [K in T]: K } {
    return values.reduce((acc, value) => {
        acc[value] = value;
        return acc;
    }, Object.create(null));
}

const names = createEnum(AVAILABLE_ICONS);
export type IconNames = keyof typeof names;

export type PhosphorIconWeight = 'thin' | 'light' | 'regular' | 'bold' | 'fill' | 'duotone';

export interface IconPropsDefaults {
    weight?: PhosphorIconWeight;
    size: FontSizeOptions;
    color: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
    rotate: RotationOptions;
    tooltipText?: string;
}

export interface IconProps extends Partial<IconPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    icon: IconNames;
    /** @deprecated No longer needed — all icons are Phosphor. Kept for backwards compatibility. */
    source?: string;
    /** @deprecated No longer needed — Phosphor uses `weight` prop instead. */
    variant?: string;
    className?: string;
}
