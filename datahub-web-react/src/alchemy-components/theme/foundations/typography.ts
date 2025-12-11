/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

const typography = {
    letterSpacings: {
        tighter: '-2px',
        tight: '-1px',
        normal: '0',
        wide: '1px',
        wider: '2px',
        widest: '4px',
    },

    lineHeights: {
        normal: 'normal',
        none: 1,
        xs: '16px',
        sm: '20px',
        md: '24px',
        lg: '28px',
        xl: '32px',
        '2xl': '36px',
        '3xl': '40px',
        '4xl': '44px',
    },

    fontWeights: {
        normal: 400, // regular
        medium: 500,
        semiBold: 600,
        bold: 700,
    },

    fonts: {
        heading: `'Mulish', -apple-system, BlinkMacSystemFont,
		'Segoe UI', Helvetica, Arial, sans-serif`,
        body: `'Mulish', -apple-system, BlinkMacSystemFont,
		'Segoe UI', Helvetica, Arial, sans-serif`,
        mono: `SFMono-Regular, Menlo, Monaco, Consolas,
		'Liberation Mono', 'Courier New', monospace`,
    },

    fontSizes: {
        xs: '10px',
        sm: '12px',
        md: '14px', // default body text size
        lg: '16px',
        xl: '18px',
        '2xl': '20px',
        '3xl': '22px',
        '4xl': '24px',
    },
};

export default typography;
