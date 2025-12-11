/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { foundations } from './foundations';

const { colors } = foundations;

export const semanticTokens = {
    colors: {
        'body-text': colors.gray[800],
        'body-bg': colors.white,
        'border-color': colors.gray[200],
        'inverse-text': colors.white,
        'subtle-bg': colors.gray[100],
        'subtle-text': colors.gray[600],
        'placeholder-color': colors.gray[500],
        primary: colors.violet[500],
        secondary: colors.blue[500],
        error: colors.red[500],
        success: colors.green[500],
        warning: colors.yellow[500],
        info: colors.blue[500],
    },
};
