import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  model() {
    // Static list of content for the index route featureCard links
    return [
      {
        title: 'Browse',
        route: 'browse',
        description: "Don't know where to start? Explore by categories."
      },
      {
        title: 'Script Finder',
        route: 'scripts',
        description: 'Want to search for a script, chain name or job name? Explore Script Finder.'
      },
      {
        title: 'Metadata Dashboard',
        route: 'metadata',
        description: 'Explore Metadata Dashboard'
      },
      {
        title: 'Schema History',
        route: 'schemahistory',
        description: 'Explore Schema History'
      },
      {
        title: 'IDPC',
        route: 'idpc',
        description: 'Explore IDPC'
      }
    ];
  }
});
