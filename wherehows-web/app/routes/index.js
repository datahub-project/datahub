import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

const { get, Route, inject: { service } } = Ember;

export default Route.extend(AuthenticatedRouteMixin, {
  /**
   * @type {Ember.Service}
   */
  sessionUser: service('current-user'),

  /**
   * Runtime application configuration options
   * @type {Ember.Service}
   */
  configurator: service(),

  /**
   * Metrics tracking service
   * @type {Ember.Service}
   */
  metrics: service(),
  model() {
    // Static list of content for the index route featureCard links
    return [
      {
        title: 'Browse',
        route: 'browse',
        alt: 'Browse Icon',
        icon: '/assets/assets/images/icons/browse.png',
        description: "Don't know where to start? Explore by categories."
      },
      {
        title: 'Script Finder',
        route: 'scripts',
        alt: 'Script Finder Icon',
        icon: '/assets/assets/images/icons/script-finder.png',
        description: 'Want to search for a script, chain name or job name? Explore Script Finder.'
      },
      {
        title: 'Metadata Dashboard',
        route: 'metadata',
        alt: 'Metadata Dashboard Icon',
        icon: '/assets/assets/images/icons/metadata.png',
        description: 'Explore Metadata Dashboard'
      },
      {
        title: 'Schema History',
        route: 'schemahistory',
        alt: 'Schema History Icon',
        icon: '/assets/assets/images/icons/schema.png',
        description: 'Explore Schema History'
      },
      {
        title: 'IDPC',
        route: 'idpc',
        alt: 'IDPC Icon',
        icon: '/assets/assets/images/icons/idpc.png',
        description: 'Explore IDPC'
      }
    ];
  },
  /**
   * Perform post model operations
   * @return {Promise}
   */
  afterModel() {
    this._super(...arguments);

    return this._trackCurrentUser();
  },

  /**
   * On entry into route, track the currently logged in user
   * @return {Promise.<void>}
   * @private
   */
  async _trackCurrentUser() {
    const { tracking = {} } = await get(this, 'configurator.getConfig')();

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    return (
      tracking.isEnabled &&
      get(this, 'sessionUser').trackCurrentUser(userId => get(this, 'metrics').identify({ userId }))
    );
  }
});
