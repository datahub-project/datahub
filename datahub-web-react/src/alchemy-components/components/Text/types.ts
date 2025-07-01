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
