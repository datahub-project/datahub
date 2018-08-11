import { splitText } from 'wherehows-web/helpers/split-text';
import { module, test } from 'qunit';

module('Unit | Helper | split text', function() {
  const defaultSeparator = '...';
  const scenarios = {
    'htmlbars-inline-precompile': 'htmlbars...ecompile',
    'Kubernetes has an ApiServer running on the master which acts as the supervisor for its cluster':
      'Kubernet...cluster',
    'a short text': 'a short text',
    '-': '-'
  };

  test('returns values', function(assert) {
    const testText = 'this is a hypothetical long  string of text';
    const runtimeSeparator = '///';

    let result = splitText(['']);
    assert.ok(typeof result === 'string', 'it returns a string type');
    assert.equal(result, '', 'it returns the passed in string');

    result = splitText([testText, 5]);
    assert.ok(typeof result === 'string', 'it returns a string type');
    assert.ok(result.includes(defaultSeparator), 'it returns a string with the default separator');

    result = splitText([]);
    assert.ok(result === '', 'it returns empty string for an undefined argument');

    result = splitText([testText, 5, runtimeSeparator]);
    assert.ok(result.includes(runtimeSeparator), 'it includes a runtime separator for truncated text');
  });

  test('it returns expected results', function(assert) {
    const expectations = Object.entries(scenarios);
    assert.expect(expectations.length);

    expectations.forEach(([input, output]) => {
      assert.equal(splitText([input]), output, `expected splitText to output ${output} for input ${input}`);
    });
  });
});
