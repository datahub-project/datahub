'use strict';

/* eslint-disable @typescript-eslint/no-var-requires */
const path = require('path');
const MergeTrees = require('broccoli-merge-trees');
const Funnel = require('broccoli-funnel');
/* eslint-disable @typescript-eslint/no-var-requires */

const productionEnv = 'production';
const mirageOptionsKey = 'mirage-from-addon';
const srcMirageDirectoryName = path.join('addon', 'mirage-addon');
const destMirageDirectoryName = path.join('mirage');
const defaultOptions = {
  includeAll: true,
  exclude: [/scenarios\/default/, /config/]
};

/**
 * Determines if the Mirage addon files should be included in the application tree, in production
 * Mirage is unused therefore we exclude them by default unless specified via the normal env build options
 * @returns Boolean
 */
const shouldIncludeAddonFiles = ({ env, _name, mirageOptions = {} }) => {
  // For prod env, check if addon file inclusion is explicitly enabled in production
  const isEnabledInProd = env === productionEnv && mirageOptions.enabled;
  // For other non prod envs, check if addon files are explicitly excluded, otherwise include
  const isNotExcludedInNonProd = env !== productionEnv && mirageOptions.excludeFilesFromBuild !== true;
  // Inclusion is enabled if this is not the dummy app, or production or in other envs, not explicitly excluded from build
  return isEnabledInProd || isNotExcludedInNonProd;
};

/**
 * Guard function checks if this is being invoked within an Ember host app or another addon
 * the `app` property is only present on addons that are a direct dependency of the application itself, not of other addons.
 * @param {EmberApp | Addon} addonOrApp reference to the addon
 */
const isInApp = addonOrApp => Boolean(addonOrApp.app);

module.exports = {
  /**
   * Configuration attribute key, which references the configuration options for handling / merging the addon files
   * @type {string}
   */
  mirageOptionsKey,

  /**
   * Name of the directory where mirage addon file are found and also where they exist in the app
   * @type {string}
   */
  srcMirageDirectoryName,
  /**
   * Complete path for the source mirage folder
   * @type {string}
   */
  srcMirageDirectoryNameComplete: '',

  /**
   * Path for the destination directory
   * @type {string}
   */
  destMirageDirectoryName,

  included(app) {
    // Properties are only necessary for use when addon is in an Ember app
    if (isInApp(this)) {
      this.mirageEnvConfig = this.app.project.config(app.env)['ember-cli-mirage'] || {};
      this.srcMirageDirectoryNameComplete = path.resolve(this.root, this.srcMirageDirectoryName);
    }
    this._super.included.call(this, app);
  },

  /**
   * Overrides the treeForApp hook to add mirage source files into the application hierarchy
   * @param {Array<Funnel>} appTree list of input nodes to be used in building the eventual application tree
   * @returns BroccoliMergeTrees
   */
  treeForApp(appTree) {
    // Only modify the tree if the addon is being loaded in a host Ember app, otherwise pass-through
    if (isInApp(this)) {
      const {
        app: { env, name, options },
        mirageOptionsKey,
        srcMirageDirectoryNameComplete,
        destMirageDirectoryName
      } = this;
      const mirageOptions = options[mirageOptionsKey] || defaultOptions;
      let trees = [appTree];

      // only include Mirage files when necessary
      if (mirageOptions && shouldIncludeAddonFiles({ env, name, mirageOptions })) {
        const { include, exclude } = mirageOptions;

        // if the expected config options are found in config
        if (mirageOptions.includeAll) {
          trees = [
            ...trees,
            new Funnel(srcMirageDirectoryNameComplete, {
              exclude: Array.isArray(exclude) ? exclude : [],
              destDir: destMirageDirectoryName
            })
          ];

          // otherwise, if only include is specified, add requested files
        } else if (Array.isArray(include)) {
          trees = [
            ...trees,
            new Funnel(srcMirageDirectoryNameComplete, {
              destDir: destMirageDirectoryName,
              include
            })
          ];
        }
      }

      return new MergeTrees(trees, {
        overwrite: true
      });
    }

    return this._super.treeForApp.call(this, appTree);
  }
};
