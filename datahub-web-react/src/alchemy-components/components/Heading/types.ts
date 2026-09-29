import { HTMLAttributes } from 'react';

import type {
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    FontWeightOptions,
} from '@components/theme/config';

import { Theme } from '@conf/theme/types';

export interface HeadingPropsDefaults {
    type: 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6';
    color: FontColorOptions;
    size: FontSizeOptions;
    weight: FontWeightOptions;
}

export interface HeadingProps extends Partial<HeadingPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    colorLevel?: FontColorLevelOptions;
}

export interface HeadingStyleProps extends Omit<HeadingPropsDefaults, 'type'>, Pick<HeadingProps, 'colorLevel'> {
    theme: Theme;
}
