import * as config from './config';
import { foundations } from './foundations';
import { semanticTokens } from './semantic-tokens';
import * as utils from './utils';

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
