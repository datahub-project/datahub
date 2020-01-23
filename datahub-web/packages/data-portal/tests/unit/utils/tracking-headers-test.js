import { module, test } from 'qunit';
import trackingHeaders, { trackingHeaderFieldsRegex } from 'wherehows-web/utils/validators/tracking-headers';
import {
  trackingHeaderList,
  nonTrackingHeaderList
} from 'wherehows-web/tests/helpers/validators/tracking-headers/constants';

module('Unit | Utility | tracking headers', function() {
  test('module exports a function', function(assert) {
    assert.expect(1);
    assert.ok(typeof trackingHeaders === 'function');
  });

  test('tracking headers utils exports a trackingHeaderFieldsRegex regular expression', function(assert) {
    assert.expect(1);
    assert.ok(trackingHeaderFieldsRegex instanceof RegExp);
  });

  test('it should correctly identify a string tracking header', function(assert) {
    assert.expect(trackingHeaderList.length);
    trackingHeaderList.forEach(candidateHeader =>
      assert.ok(trackingHeaders(candidateHeader), `${candidateHeader} is a tracking header`)
    );
  });

  test('it should correctly identify a string as NOT a tracking header', function(assert) {
    assert.expect(nonTrackingHeaderList.length);
    nonTrackingHeaderList.forEach(nonCandidate =>
      assert.notOk(trackingHeaders(nonCandidate), `${nonCandidate} is NOT a tracking header`)
    );
  });
});
