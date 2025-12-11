/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { HTMLAttributes } from 'react';

import {
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    FontWeightOptions,
    SpacingOptions,
} from '@components/theme/config';

import { Theme } from '@conf/theme/types';

export interface TextProps extends HTMLAttributes<HTMLElement> {
    type?: 'span' | 'p' | 'div' | 'pre';
    size?: FontSizeOptions;
    color?: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
    weight?: FontWeightOptions;
    lineHeight?: SpacingOptions;
    theme?: Theme;
}
