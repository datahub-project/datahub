import { TestContext } from 'ember-test-helpers';

/**
 * Start up the router for test context, allows reifying link component with application
 * routes in integration test environment
 * @link https://github.com/emberjs/ember.js/issues/16904
 * @param {TestContext} testContext the testContext to lookup the router instance
 * @returns {void}
 */
export default (testContext: TestContext): void => testContext.owner.lookup('router:main').setupRouter();
