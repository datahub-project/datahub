import { HTMLAttributes } from 'react';
import type { FontSizeOptions, FontColorOptions, FontWeightOptions, Color } from '@components/theme/config';

export interface HeadingPropsDefaults {
    type: 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6';
    color: FontColorOptions;
    size: FontSizeOptions;
    weight: FontWeightOptions;
}

export interface HeadingProps extends Partial<HeadingPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    colorLevel?: keyof Color;
}

export type HeadingStyleProps = Omit<HeadingPropsDefaults, 'type'> & Pick<HeadingProps, 'colorLevel'>;
