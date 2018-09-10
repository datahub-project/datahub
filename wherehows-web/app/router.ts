import EmberRouter from '@ember/routing/router';
import { get, getWithDefault } from '@ember/object';
import { inject } from '@ember/service';
import { scheduleOnce } from '@ember/runloop';
import config from 'wherehows-web/config/environment';
import Metrics from 'ember-metrics';

/**
 * Extracted from here: https://github.com/tildeio/router.js/blob/73d6a7f5ca6fba3cfff8460245fb5a3ceef8a2b5/lib/router/handler-info.ts#L324
 *
 * Due to import ts issues:
 *  * 2 new deps
 *  * Modify tsconfig.ts
 *  * Add RSVP definitions
 *
 * We decided to manually add the interface.
 */
interface IHandler {
  context: unknown;
  names: string[];
  name?: string;
}

const AppRouter = EmberRouter.extend({
  location: config.locationType,

  rootURL: config.rootURL,

  metrics: inject(),

  didTransition(infos: Array<IHandler>) {
    this._super(...arguments);

    // On route transition / navigation invoke page tracking
    this._trackPage(infos);
  },

  /**
   * Tracks the current page
   * @private
   */
  _trackPage(infos: Array<IHandler>) {
    scheduleOnce('afterRender', null, () => {
      //@ts-ignore types need update, location property is not a string post instantiation
      const page = get(this, 'location').getURL();
      //@ts-ignore types need update
      const title = getWithDefault(this, 'currentRouteName', 'unknown.page.title');
      const metrics = <Metrics>get(this, 'metrics');
      // Reference to the _paq queue
      //   check if we are in a browser env
      const paq = window && window._paq ? window._paq : [];
      const searchInfo = infos.find(({ name }) => name === 'search');

      /**
       * Manually track Piwik siteSearch using the `trackSiteSearch` api
       *  rather than using Piwik's default reading of url's containing the
       *  "search", "q", "query", "s", "searchword", "k" and "keyword", keywords
       * @link https://developer.piwik.org/guides/tracking-javascript-guide#internal-search-tracking
       */
      if (title.includes('search') && searchInfo) {
        const { keywords, category = 'datasets', page = 1 } = <{ [key: string]: any }>searchInfo.context;

        if (keywords) {
          // Early exit once we track search, so we do not invoke the
          //   default op by invoking `trackPageView` event
          return paq.push(['trackSiteSearch', keywords, category, page]);
        }
      }

      metrics.trackPage({ page, title });
      paq.push(['enableHeartBeatTimer']);
    });
  }
});

AppRouter.map(function() {
  this.route('page-not-found', {
    path: '/*wildcard'
  });
  this.route('datasets', function() {
    this.route(
      'dataset',
      {
        path: '/:dataset_id'
      },
      function() {
        this.route('properties');
        this.route('comments');
        this.route('schema');
        this.route('ownership');
        this.route('compliance');
        this.route('sample');
        this.route('relationships');
        this.route('access');
        this.route('health');
      }
    );
  });
  this.route('search');
  this.route('logout');

  this.route('login');

  this.route('browse', function() {
    this.route('entity', {
      path: '/:entity'
    });
  });
});

export default AppRouter;
