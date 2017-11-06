import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { run } from '@ember/runloop';

moduleForComponent('login-form', 'Integration | Component | login form', {
  integration: true
});

test('it renders', function(assert) {
  this.set('authenticateUser', () => {});
  this.render(hbs`{{login-form onSubmit=(action authenticateUser)}}`);

  assert.equal(this.$('#login-username').length, 1, 'has an input for username');
  assert.equal(this.$('#login-password').length, 1, 'has an input for password');
  assert.equal(this.$('[type=submit]').length, 1, 'has a button for submission');
});

test('triggers the onSubmit action when clicked', function(assert) {
  assert.expect(2);
  let submitActionCallCount = false;

  this.set('authenticateUser', function() {
    submitActionCallCount = true;
  });

  this.render(hbs`{{login-form onSubmit=(action authenticateUser)}}`);

  assert.equal(submitActionCallCount, false, 'submit action is not called on render');
  run(() => document.querySelector('.nacho-login-form [type=submit]').click());

  assert.equal(submitActionCallCount, true, 'submit action is called once');
});
