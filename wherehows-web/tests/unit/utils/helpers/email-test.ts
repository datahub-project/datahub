import { buildMailToUrl } from 'wherehows-web/utils/helpers/email';
import { module, test } from 'qunit';
import { emailMailToAsserts } from 'wherehows-web/tests/helpers/validators/email/email-test-fixture';

module('Unit | Utility | helpers/email', function(): void {
  test('buildMailToUrl exists', function(assert) {
    assert.equal(typeof buildMailToUrl, 'function', 'it is of function type');
  });

  test('buildMailToUrl generates expected url values', function(assert): void {
    assert.expect(emailMailToAsserts.length);

    emailMailToAsserts.forEach(({ __expected__, __args__, __assert_msg__ }) => {
      assert.equal(buildMailToUrl(__args__), __expected__, __assert_msg__);
    });
  });
});
