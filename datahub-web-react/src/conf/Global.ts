/*
   Default top-level page route names (excludes entity pages)
*/
export enum PageRoutes {
    /**
     * Server-side authentication route
     */
    ROOT = '/data-catalogue/',
    AUTHENTICATE = '/data-catalogue/authenticate',
    SIGN_UP = '/data-catalogue/signup',
    LOG_IN = '/data-catalogue/login',
    RESET_CREDENTIALS = '/data-catalogue/reset',
    SEARCH_RESULTS = '/data-catalogue/search/:type?',
    SEARCH = '/data-catalogue/search',
    BROWSE = '/data-catalogue/browse',
    BROWSE_RESULTS = '/data-catalogue/browse/:type',
    DATASETS = '/data-catalogue/datasets',
    ASSETS = '/data-catalogue/assets',
    ANALYTICS = '/data-catalogue/analytics',
    POLICIES = '/data-catalogue/policies',
    SETTINGS_POLICIES = '/data-catalogue/settings/policies',
    PERMISSIONS = '/data-catalogue/permissions',
    IDENTITIES = '/data-catalogue/identities',
    INGESTION = '/data-catalogue/ingestion',
    SETTINGS = '/data-catalogue/settings',
    DOMAINS = '/data-catalogue/domains',
    GLOSSARY = '/data-catalogue/glossary',
}

/**
 * Name of the auth cookie checked on client side (contains the currently authenticated user urn).
 */
export const CLIENT_AUTH_COOKIE = 'actor';

/**
 * Name of the unique browser id cookie generated on client side
 */
export const BROWSER_ID_COOKIE = 'bid';
