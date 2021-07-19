/// <reference types="cypress" />
// ***********************************************************
// This example plugins/index.js can be used to load plugins
//
// You can change the location of this file or turn off loading
// the plugins file with the 'pluginsFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/plugins-guide
// ***********************************************************

// This function is called when a project is opened or re-opened (e.g. due to
// the project's config changing)

const findWebpack = require('find-webpack');
const wp = require('@cypress/webpack-preprocessor');

/**
 * @type {Cypress.PluginConfig}
 */
module.exports = (on, config) => {
    // find the Webpack config used by react-scripts
    const webpackOptions = findWebpack.getWebpackOptions();

    if (!webpackOptions) {
        throw new Error('Could not find Webpack in this project 😢');
    }

    const cleanOptions = {
        reactScripts: true,
    };

    findWebpack.cleanForCypress(cleanOptions, webpackOptions);

    const options = {
        webpackOptions,
        watchOptions: {},
    };

    on('file:preprocessor', wp(options));

    // add other tasks to be registered here

    // IMPORTANT to return the config object
    // with the any changed environment variables
    return config;
};
