/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import ColorTheme from '@conf/theme/colorThemes/types';

export type Theme = {
    id: string;
    colors: ColorTheme & {
        glossaryPalette?: string[];
        domainPalette?: string[];
    };
    styles: {
        'primary-color': string;
        'primary-color-light': string;
        'primary-color-dark': string;
        'layout-header-color': string;
        'body-background': string;
        'border-color-base': string;
        'homepage-background-upper-fade': string;
        'homepage-background-lower-fade': string;
        'homepage-text-color': string;
        'box-shadow': string;
        'box-shadow-hover': string;
        'box-shadow-navbar-redesign': string;
        'border-radius-navbar-redesign': string;
        'highlight-color': string;
        'highlight-border-color': string;
        'layout-header-background'?: string;
        'layout-body-background'?: string;
        'component-background'?: string;
        'text-color'?: string;
        'text-color-secondary'?: string;
        'heading-color'?: string;
        'background-color-light'?: string;
        'divider-color'?: string;
        'disabled-color'?: string;
        'steps-nav-arrow-color'?: string;
    };
    assets: {
        logoUrl: string;
    };
    content: {
        title: string;
        subtitle?: string;
        search: {
            searchbarMessage: string;
        };
        menu: {
            items: {
                label: string;
                path: string;
                shouldOpenInNewTab: boolean;
                description?: string;
            }[];
        };
    };
};
