import React, { HTMLAttributes } from 'react';

import type {
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    RotationOptions,
} from '@components/theme/config';

export type PhosphorIconWeight = 'thin' | 'light' | 'regular' | 'bold' | 'fill' | 'duotone';

export interface IconPropsDefaults {
    size: FontSizeOptions;
    color: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
    rotate: RotationOptions;
    tooltipText?: string;
    weight?: PhosphorIconWeight;
}

export interface IconProps extends Partial<IconPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    icon: React.ComponentType<any>;
    className?: string;
}
