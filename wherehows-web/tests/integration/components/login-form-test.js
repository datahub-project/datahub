import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { run } from '@ember/runloop';

module('Integration | Component | login form', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('authenticateUser', () => {});
    await render(hbs`{{login-form onSubmit=(action authenticateUser)}}`);

    assert.equal(this.$('#login-username').length, 1, 'has an input for username');
    assert.equal(this.$('#login-password').length, 1, 'has an input for password');
    assert.equal(this.$('[type=submit]').length, 1, 'has a button for submission');
  });

  test('triggers the onSubmit action when clicked', async function(assert) {
    assert.expect(2);
    let submitActionCallCount = false;

    this.set('authenticateUser', function() {
      submitActionCallCount = true;
    });

    await render(hbs`{{login-form onSubmit=(action authenticateUser)}}`);

    assert.equal(submitActionCallCount, false, 'submit action is not called on render');
    run(() => document.querySelector('.nacho-login-form [type=submit]').click());

    assert.equal(submitActionCallCount, true, 'submit action is called once');
  });
});
