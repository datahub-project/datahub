import { HTMLAttributes } from 'react';

import { Color, FontColorOptions, FontSizeOptions, FontWeightOptions, SpacingOptions } from '@components/theme/config';

export interface TextProps extends HTMLAttributes<HTMLElement> {
    type?: 'span' | 'p' | 'div' | 'pre';
    size?: FontSizeOptions;
    color?: FontColorOptions;
    colorLevel?: keyof Color;
    weight?: FontWeightOptions;
    lineHeight?: SpacingOptions;
}
