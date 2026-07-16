import dark from '@conf/theme/colorThemes/dark';
import themeV2 from '@conf/theme/themeV2';
import { Theme } from '@conf/theme/types';

const themeV2Dark: Theme = {
    ...themeV2,
    id: 'themeV2Dark',
    colors: dark,
    styles: {
        ...themeV2.styles,
        'body-background': '#171B2B',
        'border-color-base': '#5F6685',
        'homepage-background-upper-fade': '#1E2338',
        'homepage-background-lower-fade': '#171B2B',
        'homepage-text-color': '#F5F6FA',
        'box-shadow': '0px 0px 30px 0px rgba(0, 0, 0, 0.3)',
        'box-shadow-hover': '0px 1px 0px 0.5px rgba(0, 0, 0, 0.25)',
        'box-shadow-navbar-redesign': '0 0 6px 0px rgba(0, 0, 0, 0.35)',
        'highlight-color': '#251C5E',
        'highlight-border-color': '#4B39BC80',
        'layout-header-background': '#1E2338',
        'layout-body-background': '#171B2B',
        'component-background': '#1E2338',
        'text-color': '#F5F6FA',
        'text-color-secondary': '#E9EAEE',
        'heading-color': '#F5F6FA',
        'background-color-light': '#272D48',
        'divider-color': '#5F6685',
        'disabled-color': '#8088A3',
        'steps-nav-arrow-color': '#8088A3',
    },
};

export default themeV2Dark;
