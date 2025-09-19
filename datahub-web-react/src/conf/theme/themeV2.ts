import light from '@conf/theme/colorThemes/light';
import { Theme } from '@conf/theme/types';

const themeV2: Theme = {
    id: 'themeV2',
    colors: light,
    styles: {
        'primary-color': '#533FD1',
        'primary-color-dark': '#5C3FD1',
        'primary-color-light': '#ece9f8',
        'layout-header-color': '#434343',
        'body-background': 'white',
        'border-color-base': '#ececec',
        'homepage-background-upper-fade': '#FFFFFF',
        'homepage-background-lower-fade': '#FFFFFF',
        'homepage-text-color': '#434343',
        'box-shadow': '0px 0px 30px 0px rgb(239 239 239)',
        'box-shadow-hover': '0px 1px 0px 0.5px rgb(239 239 239)',
        'box-shadow-navbar-redesign': '0 0 6px 0px rgba(93, 102, 139, 0.20)',
        'border-radius-navbar-redesign': '12px',
        'highlight-color': '#ece9f8',
        'highlight-border-color': '#07878180',
    },
    assets: {
        logoUrl: '/assets/logo.png',
    },
    content: {
        title: 'DataHub',
        search: {
            searchbarMessage: 'Find tables, dashboards, people, and more',
        },
        menu: {
            items: [
                {
                    label: 'DataHub Project',
                    path: 'https://docs.datahub.com',
                    shouldOpenInNewTab: true,
                    description: 'Explore DataHub Project website',
                },
                {
                    label: 'DataHub GitHub',
                    path: 'https://github.com/linkedin/datahub',
                    shouldOpenInNewTab: true,
                },
            ],
        },
    },
};

export default themeV2;
