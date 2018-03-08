import { datasetsUrl, datasetsCountUrl, datasetsUrlRoot } from 'wherehows-web/utils/api/datasets/shared';
import { module, test } from 'qunit';

module('Unit | Utility | api/datasets/shared');

test('datasetsCountUrl', function(assert) {
  let result = datasetsCountUrl({});
  assert.equal(result, `${datasetsUrlRoot('v2')}/count`, 'defaults to api root when no props are passed');

  result = datasetsCountUrl({ platform: 'platform' });
  assert.equal(result, `${datasetsUrlRoot('v2')}/count/platform/platform`, 'url includes platform');

  result = datasetsCountUrl({ platform: 'platform', prefix: 'prefix' });
  assert.equal(result, `${datasetsUrlRoot('v2')}/count/platform/platform/prefix/prefix`, 'url includes prefix');

  result = datasetsCountUrl({ prefix: 'prefix' });
  assert.equal(result, `${datasetsUrlRoot('v2')}/count`, 'defaults to api root when no platform is passed');
});

test('datasetsUrl', function(assert) {
  let result = datasetsUrl({});
  assert.equal(result, `${datasetsUrlRoot('v2')}?start=0`, 'builds the base url with a start query');

  result = datasetsUrl({ platform: 'platform' });
  assert.equal(result, `${datasetsUrlRoot('v2')}/platform/platform?start=0`, 'url includes platform');

  result = datasetsUrl({ platform: 'platform', prefix: 'prefix' });
  assert.equal(result, `${datasetsUrlRoot('v2')}/platform/platform/prefix/prefix?start=0`, 'url includes platform');

  result = datasetsUrl({ prefix: 'prefix' });
  assert.equal(result, `${datasetsUrlRoot('v2')}?start=0`, 'defaults to base url when no platform is passed');
});
