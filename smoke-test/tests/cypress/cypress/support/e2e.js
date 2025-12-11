/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// ***********************************************************
// This example support/index.js is processed and
// loaded automatically before your test files.
//
// This is a great place to put global configuration and
// behavior that modifies Cypress.
//
// You can change the location of this file or turn off
// automatically serving support files with the
// 'supportFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/configuration
// ***********************************************************

// Import commands.js using ES2015 syntax:
import "./commands";

// Import Testing Library commands
import "@testing-library/cypress/add-commands";
import "cypress-real-events/support";

// Alternatively you can use CommonJS syntax:
// require('./commands')

// https://github.com/bahmutov/cypress-timestamps
require("cypress-timestamps/support")({
  terminal: true, // by default the terminal output is disabled
  error: true,
  commandLog: true,
});

// Add file name to test titles for better JUnit reporting
beforeEach(function () {
  if (this.currentTest) {
    const testPath = this.currentTest.invocationDetails?.relativeFile;
    if (testPath) {
      this.currentTest.title = `${testPath}`;
    }
  }
});

afterEach(() => {
  cy.window().then((win) => {
    const browserMemoryUsage = {
      usedJSHeapSize: win.performance?.memory?.usedJSHeapSize,
      totalJSHeapSize: win.performance?.memory?.totalJSHeapSize,
      jsHeapSizeLimit: win.performance?.memory?.jsHeapSizeLimit,
    };

    cy.task("logMemoryUsage", browserMemoryUsage);
  });
});
