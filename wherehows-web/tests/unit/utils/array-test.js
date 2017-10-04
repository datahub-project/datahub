import { module, test } from 'qunit';
import { arrayMap, arrayFilter, arrayReduce, isListUnique } from 'wherehows-web/utils/array';
import { xRandomNumbers, numToString, isAString } from 'wherehows-web/tests/helpers/arrays/functions';

module('Unit | Utility | array');
const assertionLength = 10;

test('arrayMap is a function', function(assert) {
  assert.ok(typeof arrayMap === 'function', 'module exports an array map function');
});

test('arrayMap creates a valid map', function(assert) {
  assert.expect(assertionLength);

  const numbers = xRandomNumbers(assertionLength);
  const numbersToString = arrayMap(numToString);
  const strings = numbersToString(numbers);

  strings.forEach(string => assert.ok(typeof string === 'string'));
});

test('arrayFilter is a function', function(assert) {
  assert.ok(typeof arrayFilter === 'function', 'module exports an array filter function');
});

test('arrayFilter creates a valid filter', function(assert) {
  assert.expect(1);

  const numbers = xRandomNumbers(assertionLength);
  const getStrings = arrayFilter(isAString);

  assert.ok(getStrings(numbers).length === 0, 'returns a empty list of strings');
});

test('isListUnique is a function', function(assert) {
  assert.ok(typeof isListUnique === 'function', 'module exports a isListUnique function');
});

test('isListUnique correctly tests uniqueness of a list', function(assert) {
  assert.expect(2);

  const listWithDuplicateNumbers = [1, 2, 3, 4, 1];
  const listWithoutDuplicateNumbers = [1, 2, 3, 4, 5];

  assert.notOk(isListUnique(listWithDuplicateNumbers), `${listWithDuplicateNumbers} has duplicates`);
  assert.ok(isListUnique(listWithoutDuplicateNumbers), `${listWithoutDuplicateNumbers} has no duplicates`);
});

test('arrayReduce is a function', function(assert) {
  assert.ok(typeof arrayReduce === 'function', 'module exports an array reducer function');
});

test('arrayReduce should work as a reduction iteratee', function(assert) {
  const array = [{ a: 1 }, { b: 2 }, { c: 3 }],
    expected = { a: 1, b: 2, c: 3 };
  const reducer = arrayReduce(function(acc, el) {
    return { ...acc, ...el };
  }, {});

  assert.deepEqual(reducer(array), expected);
});
