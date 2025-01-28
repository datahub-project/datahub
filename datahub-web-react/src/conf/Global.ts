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
    ANALYTICS = '/analytics',
    POLICIES = '/policies',
    SETTINGS_POLICIES = '/settings/policies',
    PERMISSIONS = '/permissions',
    IDENTITIES = '/identities',
    INGESTION = '/ingestion',
    SETTINGS = '/settings',
    DOMAINS = '/domains',
    DOMAIN = '/domain',
    GLOSSARY = '/glossary',
    STRUCTURED_PROPERTIES = '/structured-properties',
    SETTINGS_VIEWS = '/settings/views',
    EMBED = '/embed',
    EMBED_LOOKUP = '/embed/lookup/:url',
    SETTINGS_POSTS = '/settings/posts',
    BUSINESS_ATTRIBUTE = '/business-attribute',
    INTRODUCE = '/introduce',
    // Temporary route to view all data products
    DATA_PRODUCTS = '/search?filter__entityType___false___EQUAL___0=DATA_PRODUCT&page=1&query=%2A&unionType=0',
}

export enum HelpLinkRoutes {
    GRAPHIQL = '/api/graphiql',
    OPENAPI = '/openapi/swagger-ui/index.html',
}

/**
 * Name of the auth cookie checked on client side (contains the currently authenticated user urn).
 */
export const CLIENT_AUTH_COOKIE = 'actor';

/**
 * Name of the unique browser id cookie generated on client side
 */
export const BROWSER_ID_COOKIE = 'bid';

/** New Routes Map for redirection */
export const NEW_ROUTE_MAP = {
    '/Validation/Assertions': '/Quality/List',
    '/Validation/Tests': '/Governance/Tests',
    '/Validation/Data%20Contract': '/Quality/Data%20Contract',
    '/Validation': '/Quality',
};
