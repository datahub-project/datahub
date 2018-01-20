import { test } from 'qunit';
import moduleForAcceptance from 'wherehows-web/tests/helpers/module-for-acceptance';
import { visit, click, find, fillIn, currentURL } from 'ember-native-dom-helpers';
import {
  loginContainer,
  authenticationUrl,
  invalidCredentials,
  testUser,
  testPasswordInvalid
} from 'wherehows-web/tests/helpers/login/constants';
import {
  loginUserInput,
  loginPasswordInput,
  loginSubmitButton
} from 'wherehows-web/tests/helpers/login/page-element-constants';

moduleForAcceptance('Acceptance | login', {
  beforeEach() {
    visit(authenticationUrl);
  }
});

test('visiting /login', function(assert) {
  andThen(function() {
    assert.equal(currentURL(), authenticationUrl, `the current url is ${authenticationUrl}`);
  });
});

test('should render login form', function(assert) {
  assert.expect(4);

  andThen(() => {
    assert.ok(find(loginContainer), 'should have a login form container');
    assert.ok(find('input[type=text]', loginContainer), 'should have a username text input field');
    assert.ok(find('input[type=password]', loginContainer), 'should have a password text input field');
    assert.ok(find('button[type=submit]', loginContainer), 'should have a submit button');
  });
});

test('should display error message with empty credentials', async function(assert) {
  assert.expect(2);
  await fillIn(loginUserInput, testUser);
  await click(loginSubmitButton);

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
  await click(loginSubmitButton);

  assert.ok(find('#login-error').textContent.length, 'error message element is rendered');

  assert.equal(
    find('#login-error').textContent.trim(),
    'Invalid Password',
    'displays invalid password message in error message container'
  );
});
