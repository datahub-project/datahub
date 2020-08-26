import { encode, decode } from '@datahub/utils/api/encode-decode-uri-component-with-space';
import { module, test } from 'qunit';

module('Unit | Utility | encode decode uri component with space', function(): void {
  const unEncodedAlphaNumericWithSpace = 'ABC 123 with space';
  const decodedAlphaNumericWithSpace = 'ABC+123+with+space';
  const reservedChars = "Aa0-_.!~*'()";

  test('encode function', function(assert): void {
    let result = encode(unEncodedAlphaNumericWithSpace);

    assert.ok(typeof result === 'string', 'encode returns a value');
    assert.notOk(/%20/.test(result), 'result does not contain %20 replacement for space');
    assert.equal(result, decodedAlphaNumericWithSpace, 'result is encoded as expected');

    result = encode(reservedChars);
    assert.equal(reservedChars, result, 'reserved characters should be untouched');
  });

  test('decode function', function(assert): void {
    const result = decode(decodedAlphaNumericWithSpace);

    assert.ok(typeof result === 'string', 'encode returns a value');
    assert.notOk(/\+/.test(result), 'result does not contain + replacement for space');
    assert.equal(result, unEncodedAlphaNumericWithSpace, 'result is decoded as expected');
  });
});
