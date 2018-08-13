'use strict';

const EmberApp = require('ember-cli/lib/broccoli/ember-app');
const Funnel = require('broccoli-funnel');
const MergeTrees = require('broccoli-merge-trees');

module.exports = function(defaults) {
  const app = new EmberApp(defaults, {
    ace: {
      modes: ['json'],
      workers: ['json'],
      exts: ['searchbox']
    },

    babel: {
      plugins: ['transform-object-rest-spread', 'transform-class-properties'],
      sourceMaps: 'inline'
    },

    'ember-cli-babel': {
      includePolyfill: true
    },

    emberHighCharts: {
      includedHighCharts: true,
      // Note: Since we only need highcharts, excluding the other available modules in the addon
      includeHighStock: false,
      includeHighMaps: false,
      includeHighChartsMore: false,
      includeHighCharts3D: false
    },

    storeConfigInMeta: false,

    SRI: {
      enabled: false
    },

    fingerprint: {
      enabled: EmberApp.env() === 'production'
    },

    'ember-cli-bootstrap-sassy': {
      js: ['dropdown', 'collapse', 'tab']
    },

    'ember-cli-uglify': {
      enabled: EmberApp.env() === 'production',
      uglify: {
        compress: {
          sequences: 20
        }
      },
      exclude: ['**/vendor.js', 'legacy-app/**']
    },

    outputPaths: {
      app: {
        html: 'index.html',

        css: {
          app: '/assets/wherehows-web.css'
        },

        js: '/assets/wherehows-web.js'
      },

      vendor: {
        css: '/assets/vendor.css',
        js: '/assets/vendor.js'
      }
    }
  });

  const faFontTree = new Funnel('bower_components/font-awesome', {
    srcDir: '/',
    include: ['**/*.woff2', '**/*.woff', '**/*.ttf', '**/font-awesome.min.css'],
    destDir: '/'
  });

  const bsFontTree = new Funnel('bower_components/bootstrap/dist/fonts', {
    srcDir: '/',
    include: ['**/*.woff2', '**/*.woff', '**/*.ttf'],
    destDir: '/assets/fonts'
  });

  const treegridImgTree = new Funnel('bower_components/jquery-treegrid/img', {
    srcDir: '/',
    include: ['**/*.png'],
    destDir: '/img'
  });

  // Use `app.import` to add additional libraries to the generated
  // output files.
  //
  // If you need to use different assets in different
  // environments, specify an object as the first parameter. That
  // object's keys should be the environment name and the values
  // should be the asset to use in that environment.
  //
  // If the library that you are including contains AMD or ES6
  // modules that you would like to import into your application
  // please specify an object with the list of modules as keys
  // along with the exports of each module as its value.

  app.import('bower_components/font-awesome/css/font-awesome.min.css');
  app.import('bower_components/json-human/css/json.human.css');
  app.import('bower_components/jquery-treegrid/css/jquery.treegrid.css');
  app.import('bower_components/jquery-jsonview/dist/jquery.jsonview.css');

  app.import('bower_components/jquery-treegrid/js/jquery.treegrid.js');
  app.import('bower_components/json-human/src/json.human.js');
  app.import('bower_components/jquery-jsonview/dist/jquery.jsonview.js');
  app.import('vendor/typeahead.jquery.js');
  app.import('bower_components/marked/marked.min.js');
  app.import('bower_components/scrollMonitor/scrollMonitor.js');
  app.import('vendor/shims/scrollmonitor.js');

  return app.toTree(new MergeTrees([faFontTree, bsFontTree, treegridImgTree]));
};
