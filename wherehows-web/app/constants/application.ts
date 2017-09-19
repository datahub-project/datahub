/**
 * Defines the props for the feedback mailto: link
 * @type {object}
 */
const feedback = { mail: 'wherehows-dev@linkedin.com', subject: 'WhereHows Feedback', title: 'Provide Feedback' };

/**
 * Defines the properties for the navigation bar avatar
 * @type {object}
 */
const avatar = {
  url: 'https://cinco.corp.linkedin.com/api/profile/[username]/picture/?access_token=2rzmbzEMGlHsszQktFY-B1TxUic',
  fallbackUrl: '/assets/assets/images/default_avatar.png'
};

/**
 * Lists the application entry points for sub features
 * @type {object}
 */
const featureEntryPoints: { [K: string]: object } = {
  browse: {
    title: 'Browse',
    route: 'browse',
    alt: 'Browse Icon',
    icon: '/assets/assets/images/icons/browse.png',
    description: "Don't know where to start? Explore by categories."
  },

  scriptFinder: {
    title: 'Script Finder',
    route: 'scripts',
    alt: 'Script Finder Icon',
    icon: '/assets/assets/images/icons/script-finder.png',
    description: 'Want to search for a script, chain name or job name? Explore Script Finder.'
  },

  metadataDashboard: {
    title: 'Metadata Dashboard',
    route: 'metadata',
    alt: 'Metadata Dashboard Icon',
    icon: '/assets/assets/images/icons/metadata.png',
    description: 'Explore Metadata Dashboard'
  },

  schemaHistory: {
    title: 'Schema History',
    route: 'schemahistory',
    alt: 'Schema History Icon',
    icon: '/assets/assets/images/icons/schema.png',
    description: 'Explore Schema History'
  },

  idpc: {
    title: 'IDPC',
    route: 'idpc',
    alt: 'IDPC Icon',
    icon: '/assets/assets/images/icons/idpc.png',
    description: 'Explore IDPC'
  }
};

export { feedback, avatar, featureEntryPoints };
