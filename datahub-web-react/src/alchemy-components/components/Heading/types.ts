/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { HTMLAttributes } from 'react';

import type {
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    FontWeightOptions,
} from '@components/theme/config';

export interface HeadingPropsDefaults {
    type: 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6';
    color: FontColorOptions;
    size: FontSizeOptions;
    weight: FontWeightOptions;
}

export interface HeadingProps extends Partial<HeadingPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    colorLevel?: FontColorLevelOptions;
}

export type HeadingStyleProps = Omit<HeadingPropsDefaults, 'type'> & Pick<HeadingProps, 'colorLevel'>;
