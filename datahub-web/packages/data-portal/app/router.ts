import EmberRouter from '@ember/routing/router';
import config from 'datahub-web/config/environment';
import { sharedRoutes } from '@datahub/shared/shared-routes';
import { entitiesRoutes } from '@datahub/entities/entities-routes';

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
  sharedRoutes(this);
  entitiesRoutes(this);

  this.route('page-not-found', {
    path: '/*wildcard'
  });

  this.route('lists', function(): void {
    this.route('entity', {
      path: '/:entity_name'
    });
  });

  this.route('search');

  this.route('logout');

  this.route('login');

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

  this.route('app-catalogue', { path: '/apps' });

  this.route('lineage', function() {
    this.route('urn', { path: '/:urn' });
  });
});
