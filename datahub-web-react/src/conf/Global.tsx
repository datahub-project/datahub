import DataHubLogo from '../images/landing-logo.png';

/*
    Reference to the Logo Image used in Log in page and in search header 
*/
export const LOGO_IMAGE = DataHubLogo;

/*
    Top-level page route names
*/
export enum PageRoutes {
    LOG_IN = '/login',
    SEARCH = '/search',
    BROWSE_TYPES = '/browse',
    BROWSE_RESULTS = '/browse/:type',
    DATASETS = '/datasets',
    USERS = '/users',
}
