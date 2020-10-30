import keyValueMapToList from 'dummy/utils/lib/key-value-map-to-list';
import { module, test } from 'qunit';
import { isArray } from '@ember/array';
import { IObject, IKeyMap } from '@nacho-ui/core/types/utils/generics';

module('Unit | Utility | lib/key-value-map-to-list', function() {
  const testMap: IObject<string> = {
    luke: 'skywalker',
    leia: 'organa',
    kylo: 'ren'
  };

  test('it works for basic case', function(assert) {
    const result = keyValueMapToList(testMap);
    assert.ok(result, 'Result exists');
    assert.ok(isArray(result), 'Returned an array');
    assert.equal(result[0].value, testMap[result[0].name], 'Key map is mapped properly');
  });

  test('it works for specified keys case', function(assert) {
    const result = keyValueMapToList(testMap, ['luke', 'leia']);
    assert.ok(isArray(result), 'Returns as array as the result');
    assert.equal(result.length, 2, 'Returns only 2 objects as our specified keys');
    assert.equal(result.filter((item: IKeyMap<string>) => item.name === 'luke').length, 1, 'Returns a key with luke');
    assert.equal(
      result.filter((item: IKeyMap<string>) => item.name === 'kylo').length,
      0,
      'Did not have a key where not included'
    );
  });
});
