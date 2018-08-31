import Authenticator from 'wherehows-web/authenticators/custom-ldap';
import { module, test } from 'qunit';
import sinon from 'sinon';
import { ApiStatus } from 'wherehows-web/utils/api';

module('Unit | Utility | authenticators/custom ldap', function(hooks) {
  hooks.beforeEach(function() {
    this.sinonServer = sinon.createFakeServer();
  });

  hooks.afterEach(function() {
    this.sinonServer.restore();
  });

  test('Authenticate methods work as expected', async function(assert) {
    assert.expect(2);

    const authenticator = new Authenticator();
    const data = {
      username: 'wherehows',
      uuid: 'wherehows-uuid'
    };

    let response;

    this.sinonServer.respondWith('POST', '/authenticate', [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({ status: ApiStatus.OK, data })
    ]);

    response = authenticator.authenticate('username', 'password');
    this.sinonServer.respond();

    assert.ok(typeof response.then === 'function', 'returns a Promise object or thennable');
    assert.equal((await response).username, data.username, 'authenticate correctly resolves with api response');
  });

  test('Restore method works as expected', function(assert) {
    const authenticator = new Authenticator();
    const response = authenticator.restore();

    assert.ok(typeof response.then === 'function', 'returns a Promise object or thennable');
  });
});
