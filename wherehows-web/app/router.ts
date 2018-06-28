import EmberRouter from '@ember/routing/router';
import { get, getWithDefault } from '@ember/object';
import { inject } from '@ember/service';
import { scheduleOnce } from '@ember/runloop';
import config from 'wherehows-web/config/environment';
import Metrics from 'ember-metrics';

const AppRouter = EmberRouter.extend({
  location: config.locationType,

  rootURL: config.rootURL,

  metrics: inject(),

  didTransition() {
    this._super(...arguments);

    // On route transition / navigation invoke page tracking
    this._trackPage();
  },

  /**
   * Tracks the current page
   * @private
   */
  _trackPage() {
    scheduleOnce('afterRender', null, () => {
      //@ts-ignore types need update, location property is not a string post instantiation
      const page = get(this, 'location').getURL();
      //@ts-ignore types need update
      const title = getWithDefault(this, 'currentRouteName', 'unknown.page.title');
      const metrics = <Metrics>get(this, 'metrics');
      // Reference to the _paq queue
      //   check if we are in a browser env
      const paq = window && window._paq ? window._paq : [];

      /**
       * Manually track Piwik siteSearch using the `trackSiteSearch` api
       *  rather than using Piwik's default reading of url's containing the
       *  "search", "q", "query", "s", "searchword", "k" and "keyword", keywords
       * @link https://developer.piwik.org/guides/tracking-javascript-guide#internal-search-tracking
       */
      if (title.includes('search')) {
        //@ts-ignore types need update
        const routerJs = get(this, 'router');
        const queryParams = routerJs ? get(routerJs, 'state.queryParams') : {};
        const { keyword, category = 'datasets', page = 1 } = queryParams;

        // Early exit once we track search, so we do not invoke the
        //   default op by invoking `trackPageView` event
        return paq.push(['trackSiteSearch', keyword, category, page]);
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
        this.route('relations');
        this.route('access');
      }
    );
  });
  this.route('search');
  this.route('metrics', function() {
    this.route('metricspage', {
      path: '/page/:page'
    });

    this.route('metric', {
      path: '/:metric_id'
    });

    this.route(
      'metricname',
      {
        path: '/name/:metric_name'
      },
      function() {
        this.route('metricnamepage', {
          path: '/page/:page'
        });

        this.route(
          'metricgroup',
          {
            path: '/:metric_group'
          },
          function() {
            this.route('metricnamesubpage', {
              path: '/page/:page'
            });
          }
        );
      }
    );
  });
  this.route('flows', function() {
    this.route('flowspage', {
      path: '/page/:page'
    });

    this.route(
      'flowsname',
      {
        path: '/name/:name'
      },
      function() {
        this.route('flowssubpage', {
          path: '/page/:page'
        });

        this.route(
          'flow',
          {
            path: '/:flow_id'
          },
          function() {
            this.route('pagedflow', {
              path: '/page/:page'
            });
          }
        );
      }
    );

    this.route('flow', {
      path: '/:flow_id'
    });
  });
  this.route('logout');
  this.route('metadata', function() {
    this.route('user', {
      path: '/:user'
    });
  });
  this.route('idpc', function() {
    this.route('user', {
      path: '/:user'
    });
  });
  this.route('scripts', function() {
    this.route('page', {
      path: '/page/:page'
    });

    this.route('script', {
      path: '/:script_id'
    });
  });

  this.route(
    'schemahistory',
    {
      path: 'schemas'
    },
    function() {
      this.route('page', {
        path: '/page/:page'
      });

      this.route('schema', {
        path: '/:schema'
      });
    }
  );
  this.route('lineage', function() {
    this.route('dataset', {
      path: '/dataset/:id'
    });
  });
  this.route('login');

  this.route('browse', function() {
    this.route('entity', {
      path: '/:entity'
    });
  });
});

export default AppRouter;
