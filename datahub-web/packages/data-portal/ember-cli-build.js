'use strict';

const EmberApp = require('ember-cli/lib/broccoli/ember-app');

/**
 * Importing required files from bootstrap
 * @param {*} app EmberApp
 */
const importBootstrap = function(app) {
  const destDir = '/assets/fonts/bootstrap';
  const boostrapFolder = 'node_modules/bootstrap/';

  ['eot', 'svg', 'ttf', 'woff', 'woff2'].forEach(extension =>
    app.import(`${boostrapFolder}fonts/glyphicons-halflings-regular.${extension}`, { destDir })
  );

  ['dropdown', 'collapse', 'tab'].forEach(fileName => app.import(`${boostrapFolder}js/${fileName}.js`));
};

module.exports = function(defaults) {
  const app = new EmberApp(defaults, {
    ace: {
      modes: ['json'],
      workers: ['json'],
      exts: ['searchbox']
    },

    babel: {
      sourceMaps: 'inline'
    },

    'ember-cli-babel': {
      includePolyfill: true
    },

    emberHighCharts: {
      includeHighCharts: true,
      // Note: Since we only need highcharts, excluding the other available modules in the addon
      includeHighStock: false,
      includeHighMaps: false,
      includeHighChartsMore: true,
      includeHighCharts3D: false,
      includeModules: ['solid-gauge']
    },

    storeConfigInMeta: false,

    SRI: {
      enabled: false
    },

    fingerprint: {
      enabled: EmberApp.env() === 'production'
    },

    'ember-cli-uglify': {
      enabled: EmberApp.env() === 'production',
      uglify: {
        compress: {
          sequences: 20
        }
      }
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
    },
    svgJar: {
      sourceDirs: ['public/assets/images/svgs']
    }
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

  app.import('node_modules/jquery-treegrid/css/jquery.treegrid.css');
  app.import('node_modules/jquery-jsonview/dist/jquery.jsonview.css');

  app.import('node_modules/jquery-treegrid/js/jquery.treegrid.js');
  app.import('node_modules/jquery-jsonview/dist/jquery.jsonview.js');
  app.import('node_modules/marked/marked.min.js');

  app.import('node_modules/vis/dist/vis.js', {
    using: [
      {
        transformation: 'amd',
        as: 'vis'
      }
    ]
  });

  app.import('node_modules/nearley/lib/nearley.js', {
    using: [{ transformation: 'cjs', as: 'nearley' }]
  });

  app.import('node_modules/restliparams/lib/index.js', {
    using: [
      {
        transformation: 'cjs',
        as: 'restliparams'
      }
    ]
  });

  importBootstrap(app);

  return app.toTree();
};
