import Authenticator from '@datahub/shared/authenticators/custom-ldap';
import { module, test } from 'qunit';
import sinon from 'sinon';
import { ApiStatus } from '@datahub/utils/api/shared';
import { TestContext } from 'ember-test-helpers';
import { SinonFakeServer } from 'sinon';

type MyTestContext = TestContext & { sinonServer: SinonFakeServer };

module('Unit | Utility | authenticators/custom ldap', function(hooks) {
  hooks.beforeEach(function(this: MyTestContext) {
    this.sinonServer = ((sinon as unknown) as { createFakeServer: () => SinonFakeServer }).createFakeServer();
  });

  hooks.afterEach(function(this: MyTestContext) {
    this.sinonServer.restore();
  });

  test('Authenticate methods work as expected', async function(this: MyTestContext, assert) {
    assert.expect(2);

    const authenticator = Authenticator.create();
    const data = {
      username: 'wherehows',
      uuid: 'wherehows-uuid'
    };

    this.sinonServer.respondWith('POST', '/authenticate', [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({ status: ApiStatus.OK, data })
    ]);

    const response = authenticator.authenticate('username', 'password');
    this.sinonServer.respond();

    assert.ok(typeof response.then === 'function', 'returns a Promise object or thennable');
    assert.equal((await response).username, data.username, 'authenticate correctly resolves with api response');
  });

  test('Restore method works as expected', function(assert) {
    const authenticator = Authenticator.create();
    const response = authenticator.restore();

    assert.ok(typeof response.then === 'function', 'returns a Promise object or thennable');
  });
});
