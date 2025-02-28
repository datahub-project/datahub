import * as config from './config';
import * as utils from './utils';

import { foundations } from './foundations';
import { semanticTokens } from './semantic-tokens';

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
