import * as config from '@components/theme/config';
import { foundations } from '@components/theme/foundations';
import { semanticTokens } from '@components/theme/semantic-tokens';
import * as utils from '@components/theme/utils';

const theme = {
    semanticTokens,
    ...foundations,
    config,
    utils,
};

export const {
    colors,
    spacing,
    radius,
    shadows,
    typography,
    breakpoints,
    zIndices,
    transform,
    transition,
    sizes,
    borders,
    blur,
} = theme;

export type Theme = typeof theme;
export default theme;
