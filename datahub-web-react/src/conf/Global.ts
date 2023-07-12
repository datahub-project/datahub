/*
   Default top-level page route names (excludes entity pages)
*/
export enum PageRoutes {
    /**
     * Server-side authentication route
     */
    ROOT = '/',
    AUTHENTICATE = '/authenticate',
    SIGN_UP = '/signup',
    LOG_IN = '/login',
    RESET_CREDENTIALS = '/reset',
    SEARCH_RESULTS = '/search/:type?',
    SEARCH = '/search',
    BROWSE = '/browse',
    BROWSE_RESULTS = '/browse/:type',
    DATASETS = '/datasets',
    ASSETS = '/assets',
    ANALYTICS = '/analytics',
    POLICIES = '/policies',
    ACTION_REQUESTS = '/requests',
    SETTINGS_POLICIES = '/settings/policies',
    PERMISSIONS = '/permissions',
    IDENTITIES = '/identities',
    INGESTION = '/ingestion',
    SETTINGS = '/settings',
    DOMAINS = '/domains',
    GLOSSARY = '/glossary',
    TESTS = '/tests',
    SETTINGS_VIEWS = '/settings/views',
    EMBED = '/embed',
    EMBED_HEALTH = '/embed/health',
    EMBED_LOOKUP = '/embed/lookup/:url',
    DATASET_HEALTH_DASHBOARD = '/observe/datasets',
}

/**
 * Name of the auth cookie checked on client side (contains the currently authenticated user urn).
 */
export const CLIENT_AUTH_COOKIE = 'actor';

/**
 * Name of the unique browser id cookie generated on client side
 */
export const BROWSER_ID_COOKIE = 'bid';
