'use strict';

const EmberApp = require('ember-cli/lib/broccoli/ember-app');
const Funnel = require('broccoli-funnel');
const MergeTrees = require('broccoli-merge-trees');

module.exports = function(defaults) {
  const app = new EmberApp(defaults, {
    babel: {
      plugins: ['transform-object-rest-spread', 'transform-class-properties'],
      sourceMaps: 'inline'
    },

    'ember-cli-babel': {
      includePolyfill: true
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

  app.import('bower_components/jquery-ui/themes/base/jquery-ui.css');
  app.import('vendor/fancytree/src/skin-wherehows/ui.wherehows.css');
  app.import('bower_components/font-awesome/css/font-awesome.min.css');
  app.import('bower_components/json-human/css/json.human.css');
  app.import('bower_components/jquery-treegrid/css/jquery.treegrid.css');
  app.import('bower_components/toastr/toastr.min.css');
  app.import('bower_components/jquery-jsonview/dist/jquery.jsonview.css');
  app.import('bower_components/jsondiffpatch/public/formatters-styles/html.css');
  app.import('bower_components/jsondiffpatch/public/formatters-styles/annotated.css');
  app.import('bower_components/x-editable/dist/bootstrap3-editable/css/bootstrap-editable.css');
  app.import('vendor/legacy_styles/main.css');
  app.import('vendor/legacy_styles/comments.css');
  app.import('vendor/legacy_styles/wherehows.css');
  app.import('vendor/legacy_styles/script-finder.css');
  app.import('vendor/legacy_styles/lineage.css');
  app.import('vendor/legacy_styles/lineage-search.css');
  app.import('vendor/dagre-d3/css/tipsy.css');

  app.import('vendor/legacy_sources/global_functions.js');
  app.import('vendor/legacy_sources/main.js');

  app.import('bower_components/jquery/dist/jquery.min.js');
  app.import('bower_components/jquery-ui/jquery-ui.min.js');
  app.import('bower_components/bootstrap/dist/js/bootstrap.min.js');
  app.import('vendor/jquery.splitter-1.6/jquery.splitter.js');
  app.import('vendor/dagre-d3/lib/lodash.min.js');
  app.import('vendor/dagre-d3/lib/d3.js');
  app.import('vendor/dagre-d3/js/utility.js');
  app.import('vendor/dagre-d3/js/dagre-d3.js');
  app.import('vendor/dagre-d3/js/tipsy.js');
  app.import('vendor/dagre-d3/js/jquery.contextMenu.js');
  app.import('vendor/d3pie-0.18/d3pie-customized.js');
  app.import('bower_components/jquery.scrollTo/jquery.scrollTo.min.js');
  app.import('bower_components/jquery-treegrid/js/jquery.treegrid.js');
  app.import('bower_components/json-human/src/json.human.js');
  app.import('bower_components/jquery-jsonview/dist/jquery.jsonview.js');
  app.import('vendor/CsvToMarkdown.js');
  app.import('vendor/typeahead.jquery.js');
  app.import('bower_components/marked/marked.min.js');
  app.import('bower_components/ace-builds/src-noconflict/ace.js');
  app.import('bower_components/ace-builds/src-noconflict/theme-github.js');
  app.import('bower_components/ace-builds/src-noconflict/mode-sql.js');
  app.import('bower_components/toastr/toastr.min.js');
  app.import('bower_components/highcharts/highcharts.js');
  app.import('bower_components/jsondiffpatch/public/build/jsondiffpatch.min.js');
  app.import('bower_components/jsondiffpatch/public/build/jsondiffpatch-formatters.min.js');
  app.import('bower_components/x-editable/dist/bootstrap3-editable/js/bootstrap-editable.min.js');
  app.import('bower_components/scrollMonitor/scrollMonitor.js');
  app.import('vendor/shims/scrollmonitor.js');

  return app.toTree(new MergeTrees([faFontTree, bsFontTree, treegridImgTree]));
};
