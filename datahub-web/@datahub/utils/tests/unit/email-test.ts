import { buildMailToUrl } from '@datahub/utils/helpers/email';
import { module, test } from 'qunit';
import { emailMailToAsserts } from 'dummy/tests/helpers/email/email-test-fixture';

module('Unit | Utility | helpers/email', function(): void {
  test('buildMailToUrl exists', function(assert): void {
    assert.equal(typeof buildMailToUrl, 'function', 'it is of function type');
  });

  test('buildMailToUrl generates expected url values', function(assert): void {
    assert.expect(emailMailToAsserts.length);

    emailMailToAsserts.forEach(({ expected, args, assertMsg }) => {
      assert.equal(buildMailToUrl(args), expected, assertMsg);
    });
  });
});
