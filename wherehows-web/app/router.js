import Ember from 'ember';
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType,
  rootURL: config.rootURL
});

Router.map(function() {
  this.route('page-not-found', {
    path: '/*wildcard'
  });
  this.route('datasets', function() {
    this.route('page', {
      path: '/page/:page'
    });

    this.route('dataset', {
      path: '/:dataset_id'
    });

    this.route('name', {
      path: '/name/:name'
    }, function() {
      this.route('subpage', {
        path: '/page/:page'
      });
    });
  });
  this.route('search');
  this.route('advsearch');
  this.route('metrics', function() {
    this.route('metricspage', {
      path: '/page/:page'
    });

    this.route('metric', {
      path: '/:metric_id'
    });

    this.route('metricname', {
      path: '/name/:metric_name'
    }, function() {
      this.route('metricnamepage', {
        path: '/page/:page'
      });

      this.route('metricgroup', {
        path: '/:metric_group'
      }, function() {
        this.route('metricnamesubpage', {
          path: '/page/:page'
        });
      });
    });
  });
  this.route('flows', function() {
    this.route('flowspage', {
      path: '/page/:page'
    });

    this.route('flowsname', {
      path: '/name/:name'
    }, function() {
      this.route('flowssubpage', {
        path: '/page/:page'
      });

      this.route('flow', {
        path: '/:flow_id'
      }, function() {
        this.route('pagedflow', {
          path: '/page/:page'
        });
      });
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

  this.route('schemahistory', {
    path: 'schemas'
  }, function() {
    this.route('page', {
      path: '/page/:page'
    });

    this.route('schema', {
      path: '/:schema'
    });
  });
  this.route('lineage', function() {
    this.route('dataset', {
      path: '/dataset/:id'
    });
  });
  this.route('login');

  this.route('help', function() {
    this.route('feedback');
  });
});

export default Router;
