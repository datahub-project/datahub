import { test } from 'qunit';
import moduleForAcceptance from 'wherehows-web/tests/helpers/module-for-acceptance';
import { authenticationUrl, testUser, testPassword } from 'wherehows-web/tests/helpers/login/constants';
import {
  loginUserInput,
  loginPasswordInput,
  loginSubmitButton
} from 'wherehows-web/tests/helpers/login/page-element-constants';

moduleForAcceptance('Acceptance | browse', {
  beforeEach() {
    visit(authenticationUrl);
    fillIn(loginUserInput, testUser);
    fillIn(loginPasswordInput, testPassword);
    click(loginSubmitButton);
  }
});

test('Verify the broswe icon exist', function(assert) {
  visit('/');

  andThen(function() {
    assert.equal(
      find(`${'.feature-card__title'}:eq(0)`)
        .text()
        .trim(),
      'Browse'
    );
  });
});
