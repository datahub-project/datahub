/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ButtonHTMLAttributes } from 'react';

import { IconProps } from '@components/components/Icon/types';
import type { ColorOptions, SizeOptions } from '@components/theme/config';

import { Theme } from '@src/conf/theme/types';

export enum ButtonVariantValues {
    filled = 'filled',
    outline = 'outline',
    text = 'text',
    secondary = 'secondary',
    link = 'link',
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
