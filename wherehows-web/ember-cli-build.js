/*jshint node:true*/
/* global require, module */
const EmberApp = require('ember-cli/lib/broccoli/ember-app');
const Funnel = require('broccoli-funnel');
const MergeTrees = require('broccoli-merge-trees');

module.exports = function(defaults) {
  const app = new EmberApp(defaults, {
    'ember-cli-bootstrap-sassy': {
      js: ['dropdown', 'collapse', 'tab']
    },
    minifyJS: {
      options: {
        exclude: ['**/vendor.js', 'legacy-app/**']
      }
    }
  });

  const faFontTree = new Funnel('bower_components/font-awesome/fonts', {
    srcDir: '/',
    include: ['**/*.woff2', '**/*.woff', '**/*.ttf'],
    destDir: '/fonts'
  });

  const bsFontTree = new Funnel('bower_components/bootstrap/dist/fonts', {
    srcDir: '/',
    include: ['**/*.woff2', '**/*.woff', '**/*.ttf'],
    destDir: '/fonts'
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
  app.import(
    'bower_components/jquery.fancytree/dist/skin-win8/ui.fancytree.min.css'
  );
  app.import('vendor/fancytree/src/skin-wherehows/ui.wherehows.css');
  app.import('bower_components/font-awesome/css/font-awesome.min.css');
  app.import('bower_components/json-human/css/json.human.css');
  app.import('bower_components/jquery-treegrid/css/jquery.treegrid.css');
  app.import('bower_components/toastr/toastr.min.css');
  app.import('bower_components/jquery-jsonview/dist/jquery.jsonview.css');
  // app.import('bower_components/bootstrap/dist/css/bootstrap.min.css');
  app.import(
    'bower_components/jsondiffpatch/public/formatters-styles/html.css'
  );
  app.import(
    'bower_components/jsondiffpatch/public/formatters-styles/annotated.css'
  );
  app.import('vendor/legacy_styles/main.css');
  app.import('vendor/legacy_styles/comments.css');
  app.import('vendor/legacy_styles/wherehows.css');
  app.import('vendor/legacy_styles/script-finder.css');
  app.import('vendor/legacy_styles/lineage.css');
  app.import('vendor/legacy_styles/lineage-search.css');
  app.import('vendor/dagre-d3/css/tipsy.css');

  app.import('vendor/legacy_sources/global_functions.js');
  app.import('vendor/legacy_sources/search.js');
  app.import('vendor/legacy_sources/tree.js');
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
  app.import('bower_components/jquery.fancytree/dist/jquery.fancytree.min.js');
  app.import(
    'bower_components/jquery.fancytree/dist/src/jquery.fancytree.filter.js'
  );
  app.import('bower_components/jquery.scrollTo/jquery.scrollTo.min.js');
  app.import('bower_components/jquery-treegrid/js/jquery.treegrid.js');
  app.import('bower_components/json-human/src/json.human.js');
  app.import('bower_components/jquery-jsonview/dist/jquery.jsonview.js');
  app.import('bower_components/CsvToMarkdownTable/src/CsvToMarkdown.js');
  app.import('bower_components/marked/marked.min.js');
  app.import('bower_components/ace-builds/src-min/ace.js');
  app.import('bower_components/ace-builds/src-min/theme-github.js');
  app.import('bower_components/ace-builds/src-min/mode-sql.js');
  app.import('bower_components/toastr/toastr.min.js');
  app.import('bower_components/highcharts/highcharts.js');
  app.import(
    'bower_components/jsondiffpatch/public/build/jsondiffpatch.min.js'
  );
  app.import(
    'bower_components/jsondiffpatch/public/build/jsondiffpatch-formatters.min.js'
  );

  return app.toTree(new MergeTrees([faFontTree, bsFontTree, treegridImgTree]));
};
