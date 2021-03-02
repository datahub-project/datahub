import DataHubLogo from '../images/landing-logo.png';
/*
    Reference to the Logo Image used in Log in page and in search header 
*/
export const LOGO_IMAGE = DataHubLogo;

/*
   Default top-level page route names (excludes entity pages)
*/
export enum PageRoutes {
    LOG_IN = '/login',
    SEARCH_RESULTS = '/search/:type?',
    SEARCH = '/search',
    BROWSE = '/browse',
    BROWSE_RESULTS = '/browse/:type',
    DATASETS = '/datasets',
    ASSETS = '/assets',
}
