import { visit, click, find, fillIn, currentURL, settled } from '@ember/test-helpers';
import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import {
  loginContainer,
  authenticationUrl,
  invalidCredentials,
  testUser,
  testPasswordInvalid,
  testVipToken,
  testPassword
} from 'wherehows-web/tests/helpers/login/constants';
import {
  loginUserInput,
  loginPasswordInput,
  loginSubmitButton,
  loginVipTokenInput
} from 'wherehows-web/tests/helpers/login/page-element-constants';

module('Acceptance | login', function(hooks) {
  setupApplicationTest(hooks);

  hooks.beforeEach(async function() {
    await visit(authenticationUrl);
  });

  test('visiting /login', function(assert) {
    assert.equal(currentURL(), authenticationUrl, `the current url is ${authenticationUrl}`);
  });

  test('should render login form', function(assert) {
    assert.expect(5);

    assert.ok(find(loginContainer), 'should have a login form container');
    assert.ok(find('input[type=text]'), 'should have a username text input field');
    assert.ok(find('input[type=password]'), 'should have a password text input field');
    assert.ok(find('input[type=number]'), 'should have a VIP Token input field');
    assert.ok(find('button[type=submit]'), 'should have a submit button');
  });

  test('should display error message with empty credentials', async function(assert) {
    assert.expect(2);
    await fillIn(loginUserInput, testUser);
    await click(loginSubmitButton);
    await settled();
    assert.ok(find('#login-error').textContent.length, 'error message element is rendered');

    assert.equal(
      find('#login-error').textContent.trim(),
      invalidCredentials,
      'displays missing or invalid credentials message'
    );
  });

  test('should display invalid password message with invalid password entered', async function(assert) {
    assert.expect(2);
    await fillIn(loginUserInput, testUser);
    await fillIn(loginPasswordInput, testPasswordInvalid);
    await fillIn(loginVipTokenInput, testVipToken);
    await click(loginSubmitButton);
    await settled();
    assert.ok(find('#login-error').textContent.length, 'error message element is rendered');

    assert.equal(
      find('#login-error').textContent.trim(),
      'Invalid Password',
      'displays invalid password message in error message container'
    );
  });

  /**
   * Treating empty VIP as invalid password,
   * since they are part of the same authentication request to the LDAP server.
   */
  test('should display error message with empty VIP Token', async function(assert) {
    assert.expect(2);
    await fillIn(loginUserInput, testUser);
    await fillIn(loginPasswordInput, testPassword);
    await click(loginSubmitButton);
    await settled();
    assert.ok(find('#login-error').textContent.length, 'error message element is rendered');

    assert.equal(
      find('#login-error').textContent.trim(),
      'Invalid Password',
      'displays invalid password message in error message container'
    );
  });
});
