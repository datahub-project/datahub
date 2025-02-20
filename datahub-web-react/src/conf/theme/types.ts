export type Theme = {
    styles: {
        'primary-color'?: string;
        'layout-header-background': string;
        'layout-header-color': string;
        'layout-body-background': string;
        'component-background': string;
        'body-background': string;
        'border-color-base': string;
        'text-color': string;
        'text-color-secondary': string;
        'heading-color': string;
        'background-color-light': string;
        'divider-color': string;
        'disabled-color': string;
        'steps-nav-arrow-color': string;
        'homepage-background-upper-fade': string;
        'homepage-background-lower-fade': string;
        'box-shadow': string;
        'box-shadow-hover': string;
        'highlight-color': string;
        'highlight-border-color': string;
    };
    assets: {
        logoUrl: string;
    };
    content: {
        title: string;
        subtitle?: string;
        homepage: {
            homepageMessage: string;
        };
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
