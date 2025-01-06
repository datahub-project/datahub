import { HTMLAttributes } from 'react';
import type { FontSizeOptions, FontColorOptions, FontWeightOptions } from '@components/theme/config';

export interface HeadingProps extends HTMLAttributes<HTMLElement> {
    type?: 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6';
    size?: FontSizeOptions;
    color?: FontColorOptions;
    weight?: FontWeightOptions;
}
