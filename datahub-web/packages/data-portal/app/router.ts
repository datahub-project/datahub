import EmberRouter from '@ember/routing/router';
import config from 'wherehows-web/config/environment';

/**
 * Extends the EmberRouter object to define application routes and track events cross application
 * @class ApplicationRouter
 * @extends {EmberRouter}
 */
class ApplicationRouter extends EmberRouter {
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

  this.route('user', function(): void {
    this.route(
      'entity',
      {
        path: '/entity/:entity'
      },
      function(): void {
        this.route('own');
      }
    );
    this.route('profile', { path: '/:user_id' }, function(): void {
      this.route('tab', { path: '/:tab_selected' });
    });
  });
});

export default ApplicationRouter;
