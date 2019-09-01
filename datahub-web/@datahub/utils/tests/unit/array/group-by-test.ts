import { groupBy } from '@datahub/utils/array/group-by';
import { module, test } from 'qunit';

module('Unit | Utility | array/group-by', function() {
  test('it groups by key', function(assert) {
    const element = {
      key: 'aStringValue'
    };
    const keyName = element.key;
    const expectedGroupedResult = {
      [keyName]: [element]
    };
    let actualGroupedResult = groupBy([element], 'key');

    assert.deepEqual(actualGroupedResult, expectedGroupedResult, `expected result to have a list keyed by ${keyName}`);

    let elements = [element, element];
    actualGroupedResult = groupBy(elements, 'key');

    assert.ok(Array.isArray(actualGroupedResult.aStringValue), `expected ${keyName} in result to be of type array`);

    assert.equal(
      actualGroupedResult.aStringValue.length,
      elements.length,
      `expected result key ${keyName} array length to be ${elements.length}`
    );

    const thirdElement = { key: 'bStringValue' };
    elements = [...elements, thirdElement];
    actualGroupedResult = groupBy(elements, 'key');

    assert.ok(
      Array.isArray(actualGroupedResult.aStringValue) && Array.isArray(actualGroupedResult.bStringValue),
      `expected ${keyName} ${thirdElement.key} to arrays on the result from groupBy`
    );
    assert.equal(actualGroupedResult.bStringValue.length, 1, `expected a length of 1 for array at ${thirdElement.key}`);
    const expectedGroupedResultB = {
      [thirdElement.key]: [thirdElement]
    };
    assert.deepEqual(
      actualGroupedResult[thirdElement.key],
      expectedGroupedResultB[thirdElement.key],
      `expected result list to deeply equal expectedGroupedResultB list`
    );
  });
});
