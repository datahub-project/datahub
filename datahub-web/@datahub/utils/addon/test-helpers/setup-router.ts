import { TestContext } from 'ember-test-helpers';
import EmberRouter from '@ember/routing/router';

/**
 * extracts the first argument
 */
type ArgsType<T> = T extends (arg: infer U) => unknown ? U : never;

/**
 * Start up the router for test context, allows reifying link component with application
 * routes in integration test environment
 * @link https://github.com/emberjs/ember.js/issues/16904
 * @param {TestContext} testContext the testContext to lookup the router instance
 * @returns {void}
 */
export default (testContext: TestContext, customRoutes?: ArgsType<typeof EmberRouter['map']>): void => {
  if (customRoutes) {
    class MyCustomRouter extends EmberRouter {}
    MyCustomRouter.map(customRoutes);
    testContext.owner.register('router:main', MyCustomRouter);
  }
  testContext.owner.lookup('router:main').setupRouter();
};
