import EmberRouter from '@ember/routing/router';
import Metrics from 'ember-metrics';
import { getPiwikActivityQueue } from 'wherehows-web/utils/analytics/piwik';
import { inject as service } from '@ember/service';
import { on } from '@ember-decorators/object';
import Transition from 'wherehows-web/typings/modules/routerjs';
import Route from '@ember/routing/route';
import config from 'wherehows-web/config/environment';
import { mapOfRouteNamesToResolver, resolveDynamicRouteName } from 'wherehows-web/utils/helpers/routes';
import { IEmberLocation } from 'wherehows-web/typings/custom-ember';
import { searchRouteName } from 'wherehows-web/constants/analytics/site-search-tracking';

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

  /**
   * The ember-metrics service to object to track interesting events on route transitions e.g. page view
   * @type {Metrics}
   * @memberof ApplicationRouter
   */
  @service
  metrics: Metrics;

  /**
   * Bind to the routeDidChange event to track global successful route transitions and track page view on metrics service
   * @param {Transition<any>} transition thennable representing an attempt to transition to another route
   * @memberof ApplicationRouter
   */
  @on('routeDidChange')
  trackPageView({ to }: Transition<Route>): void {
    // https://github.com/emberjs/ember.js/blob/8b0c0006a8b9e6f5aec839cd14ca0e27feef19cb/packages/%40ember/-internals/routing/lib/location/hash_location.ts#L72
    // types outdated, location property is not a string post instantiation
    const page = ((this.location as unknown) as IEmberLocation).getURL();
    const metrics = this.metrics;
    // fallback to page value if a resolution cannot be determined, e.g when to / from is null
    const title = resolveDynamicRouteName(mapOfRouteNamesToResolver, to) || page;
    const isSearchRoute =
      title.includes(searchRouteName) || (to && to.find(({ name }): boolean => name === searchRouteName));

    if (!isSearchRoute) {
      // Track a page view event only on page's / route's that are not search
      metrics.trackPage({ page, title });
    }

    getPiwikActivityQueue().push(['enableHeartBeatTimer']);
  }
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
    this.route(
      'entity',
      {
        path: '/entity/:entity'
      },
      function(): void {
        this.route('own');
      }
    );
    this.route('profile', { path: '/:user_id' });
  });
});

export default ApplicationRouter;
