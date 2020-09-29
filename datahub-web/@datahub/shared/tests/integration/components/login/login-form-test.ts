import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { run } from '@ember/runloop';
import { noop } from 'lodash';

module('Integration | Component | login form', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('authenticateUser', noop);
    await render(hbs`<Login::LoginForm @onSubmit={{fn this.authenticateUser}}/>`);

    assert.dom('#login-username').exists({ count: 1 }, 'has an input for username');
    assert.dom('#login-password').exists({ count: 1 }, 'has an input for password');
    assert.dom('[type=submit]').exists({ count: 1 }, 'has a button for submission');
  });

  test('triggers the onSubmit action when clicked', async function(assert) {
    assert.expect(2);
    let submitActionCallCount = false;

    this.set('authenticateUser', function() {
      submitActionCallCount = true;
    });

    await render(hbs`<Login::LoginForm @onSubmit={{fn this.authenticateUser}}/>`);

    assert.equal(submitActionCallCount, false, 'submit action is not called on render');
    run(() => document.querySelector<HTMLFormElement>('.nacho-login-form [type=submit]')?.click());

    assert.equal(submitActionCallCount, true, 'submit action is called once');
  });
});
