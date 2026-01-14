import { AnchorHTMLAttributes } from 'react';

import type { ColorOptions, FontColorLevelOptions } from '@components/theme/config';

export interface LinkPropsDefaults {
    color: ColorOptions;
    colorLevel?: FontColorLevelOptions;
    target: string;
    rel: string;
}

export interface LinkProps extends Partial<LinkPropsDefaults>, Omit<AnchorHTMLAttributes<HTMLAnchorElement>, 'color'> {}

export interface LinkStyleProps {
    color: ColorOptions;
    colorLevel?: FontColorLevelOptions;
}
