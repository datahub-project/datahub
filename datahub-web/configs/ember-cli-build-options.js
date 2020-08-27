'use strict';

/**
 * Importing required files from bootstrap
 * @param {*} app emberApp
 */
const importBootstrap = function(app) {
  const destDir = '/assets/fonts/bootstrap';
  const boostrapFolder = 'node_modules/bootstrap/';

  ['eot', 'svg', 'ttf', 'woff', 'woff2'].forEach(extension =>
    app.import(`${boostrapFolder}fonts/glyphicons-halflings-regular.${extension}`, { destDir })
  );

  ['dropdown', 'collapse', 'tab'].forEach(fileName => app.import(`${boostrapFolder}js/${fileName}.js`));
};

/**
 * Configuration options for Ember CLI App used to manage broccoli build tree for DataHub web.
 * Returns a method to import build dependencies and an options
 * object with configuration  attributes
 *
 * @param {string} env current build application environment
 * @returns { importBootstrap: Function; options: object }
 */
module.exports = function(env) {
  const isTesting = env === 'test';
  const isProduction = env === 'production';

  return {
    /**
     * Provides the options to import bootstrap dependencies at build time
     */
    importBootstrap,
    options: {
      // Configuration options for ember-auto-import library
      autoImport: {
        // Note: restliparams has an outDir of lib, but autoImport looks for dist
        alias: {
          restliparams: 'restliparams/lib'
        },
        webpack: {
          node: {
            // this will add support for 'require('path')' in browsers
            // this is needed by minimatch dependency
            path: true
          }
        }
      },

      // Configurations options for ember-ace editor library
      ace: isTesting
        ? {}
        : {
            modes: ['json', 'graphqlschema', 'text'],
            workers: ['json', 'graphqlschema', 'text'],
            exts: ['searchbox']
          },

      babel: {
        sourceMaps: env === 'development' ? 'inline' : false,
        targets: {
          browsers: ['last 3 versions']
        }
      },

      'ember-cli-babel': {
        includePolyfill: !isTesting
      },

      storeConfigInMeta: false,

      SRI: {
        enabled: false
      },

      fingerprint: {
        enabled: isProduction
      },

      'ember-cli-uglify': {
        enabled: isProduction,
        // Improve build times by using the Fast Minify Mode
        // For our internal use case, app load times are not a significant bottleneck currently
        // https://github.com/mishoo/UglifyJS2#uglify-fast-minify-mode
        uglify: {
          compress: false,
          mangle: true
        }
      },

      outputPaths: {
        app: {
          html: 'index.html',

          css: {
            app: '/assets/datahub-web.css'
          },

          js: '/assets/datahub-web.js'
        },

        vendor: {
          css: '/assets/vendor.css',
          js: '/assets/vendor.js'
        }
      },

      svgJar: {
        sourceDirs: ['public/assets/images/svgs']
      },

      // Configuration options specifying inclusion of Mirage addon files in the application tree
      'mirage-from-addon': {
        includeAll: true,
        exclude: [/scenarios\/default/, /config/]
      }
    }
  };
};
