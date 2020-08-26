import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { AuthenticationType } from '@datahub/shared/constants/authentication/auth-type';

module('Integration | Component | sso/login-button', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    assert.expect(1);
    this.set('authenticateUser', (authType: AuthenticationType) => {
      assert.equal(authType, AuthenticationType.Sso);
    });

    await render(hbs`<Sso::LoginButton @authenticateUser={{fn this.authenticateUser}} />`);
    await click('button');
  });
});
