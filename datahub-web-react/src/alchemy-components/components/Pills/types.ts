/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { HTMLAttributes } from 'react';

import { IconSource } from '@src/alchemy-components/components/Icon/types';
import { ColorOptions, PillVariantOptions, SizeOptions } from '@src/alchemy-components/theme/config';
import { Theme } from '@src/conf/theme/types';

export interface PillPropsDefaults {
    variant: PillVariantOptions;
    size: SizeOptions;
    color: ColorOptions;
    clickable: boolean;
    theme?: Theme;
}

export interface PillProps extends Partial<PillPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    label: string;
    color?: ColorOptions;
    rightIcon?: string;
    leftIcon?: string;
    iconSource?: IconSource;
    customStyle?: React.CSSProperties;
    showLabel?: boolean;
    customIconRenderer?: () => void;
    onClickRightIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onClickLeftIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onPillClick?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    className?: string;
    dataTestId?: string;
}

export type PillStyleProps = PillPropsDefaults & Pick<PillProps, 'color'>;
