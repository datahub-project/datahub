import EmberRouter from '@ember/routing/router';
import config from 'datahub-web/config/environment';

/**
 * Extends the EmberRouter object to define application routes and track events cross application
 * @class ApplicationRouter
 * @extends {EmberRouter}
 */
export default class ApplicationRouter extends EmberRouter {
  /**
   * Sets application root prefix for all routes
   * @type {string}
   * @memberof ApplicationRouter
   */
  rootURL = config.rootURL;

  /**
   * Sets the type of URL that the application will use e.g. HashLocation
   * On instantiation this property is a string but post-instantiation is mutated to a IEmberLocation instance
   * @type {string}
   * @memberof ApplicationRouter
   */
  location = config.locationType;
}

ApplicationRouter.map(function(): void {
  this.route('entity-type', { path: '/:entity_type' }, function(): void {
    this.route('urn', { path: '/:urn' }, function(): void {
      this.route('tab', { path: '/:tab_selected' });
    });
  });

  this.route('page-not-found', {
    path: '/*wildcard'
  });

  this.route('datasets', function(): void {
    this.route(
      'dataset',
      {
        path: '/:dataset_urn'
      },
      function(): void {
        this.route('tab', {
          path: '/:tab_selected'
        });
      }
    );
  });

  this.route('features', function(): void {
    this.route(
      'feature',
      {
        path: '/:feature_urn'
      },
      function(): void {
        this.route('tab', {
          path: '/:tab_selected'
        });
      }
    );
  });

  // Adds the top level route for data jobs, navigated by the specific urn
  // Also adds the instance sub route for a specific job and the anchor route for a job tab
  this.route('jobs', function(): void {
    this.route(
      'job',
      {
        path: '/:job_urn'
      },
      function(): void {
        this.route('tab', {
          path: '/:tab_selected'
        });
      }
    );
  });

  this.route('lists', function(): void {
    this.route('entity', {
      path: '/:entity_name'
    });
  });

  this.route('search');

  this.route('logout');

  this.route('login');

  this.route('retina-authoring');

  this.route('browse', function(): void {
    this.route('entity', {
      path: '/:entity'
    });
  });

  this.route('browsesearch', function(): void {
    this.route('entity', {
      path: '/:entity'
    });
  });

  this.route('user', function(): void {
    this.route('profile', { path: '/:user_id' }, function(): void {
      this.route('tab', { path: '/:tab_selected' });
    });
  });

  this.route('app-catalogue', { path: '/apps' });

  this.route('dataconcepts', function(): void {
    this.route('dataconcept', { path: '/:concept_urn' }, function(): void {
      this.route('tab', { path: '/:tab_selected' });
    });
  });

  this.route('lineage', function() {
    this.route('urn', { path: '/:urn' });
  });

  // TODO: META-10716 Investigate and implement approach to agnostically include routes from monorepo addon packages
  // Index and top-level route are not required for any current use cases, therefore not implemented
  this.route('pending-proposals', { path: 'pending-proposals/:urn' });
  this.route('schema-graph', function() {
    this.route('urn', { path: '/:urn' });
  });
});
