import Sinon from 'sinon';

/**
 * Local convenience interface for return value from setupSinonTest
 * @interface ISinonRequested
 */
interface ISinonRequested {
  request?: Sinon.SinonFakeXMLHttpRequest;
  requester: Sinon.SinonFakeXMLHttpRequestStatic;
}

/**
 * Convenience function to setup environment for sinon test
 *
 * @param {SinonTestContext} context a reference to the sinon test case's this
 * @returns {ISinonRequested}
 */
export const setupSinonTest = (context: SinonTestContext): ISinonRequested => {
  const setupValue: ISinonRequested = { request: undefined, requester: context.sandbox.useFakeXMLHttpRequest() };
  setupValue.requester.onCreate = req => {
    setupValue.request = req;
  };

  return setupValue;
};
