/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { HTMLAttributes } from 'react';

import { AVAILABLE_ICONS } from '@components/components/Icon/constants';
import type {
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    RotationOptions,
} from '@components/theme/config';

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
export type PhosphorIconWeight = 'thin' | 'light' | 'regular' | 'bold' | 'fill' | 'duotone';
export type IconSource = 'material' | 'phosphor';

export interface IconPropsDefaults {
    source: IconSource;
    variant: MaterialIconVariant;
    weight?: PhosphorIconWeight;
    size: FontSizeOptions;
    color: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
    rotate: RotationOptions;
    tooltipText?: string;
}

export interface IconProps extends Partial<IconPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    icon: IconNames;
    className?: string;
}
