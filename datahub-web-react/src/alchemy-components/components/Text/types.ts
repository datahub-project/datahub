import { HTMLAttributes } from 'react';
import type { FontSizeOptions, FontColorOptions, FontWeightOptions } from '@components/theme/config';

export interface TextProps extends HTMLAttributes<HTMLElement> {
    type?: 'span' | 'p' | 'div';
    size?: FontSizeOptions;
    color?: FontColorOptions;
    weight?: FontWeightOptions;
}
