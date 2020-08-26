import Ember from 'ember';

// Note: This declaration is required as ember typings do not include the application instance on
// the test context, however in their generated files for instance-initializers tests there is
// a generated this.instance code
declare module 'ember-test-helpers' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface TestContext {
    instance: Ember.ApplicationInstance;
  }
}
