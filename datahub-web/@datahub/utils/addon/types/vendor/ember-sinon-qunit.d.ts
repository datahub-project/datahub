/**
 * Documents interface for third party module ember-sinon-qunit/test-support/test
 * As well as additional interface definition for testContext
 */
declare module 'ember-sinon-qunit/test-support/test' {
  import { test } from 'qunit';
  export default test;
}

/**
 * Defines the type of this in a sinon test case
 */
type SinonTestContext = import('ember-test-helpers').TestContext & {
  sandbox: import('sinon').SinonSandbox;
} & import('sinon').SinonSandbox;
