/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
