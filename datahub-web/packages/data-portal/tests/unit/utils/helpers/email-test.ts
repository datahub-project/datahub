import { buildMailToUrl } from 'wherehows-web/utils/helpers/email';
import { module, test } from 'qunit';
import { emailMailToAsserts } from 'wherehows-web/tests/helpers/validators/email/email-test-fixture';

module('Unit | Utility | helpers/email', function(): void {
  test('buildMailToUrl exists', function(assert) {
    assert.equal(typeof buildMailToUrl, 'function', 'it is of function type');
  });

  test('buildMailToUrl generates expected url values', function(assert): void {
    assert.expect(emailMailToAsserts.length);

    emailMailToAsserts.forEach(({ expected, args, assertMsg }) => {
      assert.equal(buildMailToUrl(args), expected, assertMsg);
    });
  });
});
