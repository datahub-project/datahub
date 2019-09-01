import { datasetsUrl } from 'wherehows-web/utils/api/datasets/shared';
import { module, test } from 'qunit';
import { datasetsCountUrl } from '@datahub/data-models/api/dataset/count';
import { datasetUrlRoot } from '@datahub/data-models/api/dataset/dataset';
import { ApiVersion } from '@datahub/utils/api/shared';

module('Unit | Utility | api/datasets/shared', function() {
  test('datasetsCountUrl', function(assert) {
    let result = datasetsCountUrl({});
    assert.equal(result, `${datasetUrlRoot(ApiVersion.v2)}/count`, 'defaults to api root when no props are passed');

    result = datasetsCountUrl({ platform: 'platform' });
    assert.equal(result, `${datasetUrlRoot(ApiVersion.v2)}/count/platform/platform`, 'url includes platform');

    result = datasetsCountUrl({ platform: 'platform', prefix: 'prefix' });
    assert.equal(
      result,
      `${datasetUrlRoot(ApiVersion.v2)}/count/platform/platform/prefix/prefix`,
      'url includes prefix'
    );

    result = datasetsCountUrl({ prefix: 'prefix' });
    assert.equal(result, `${datasetUrlRoot(ApiVersion.v2)}/count`, 'defaults to api root when no platform is passed');
  });

  test('datasetsUrl', function(assert) {
    // @ts-ignore
    let result = datasetsUrl({});
    assert.equal(result, `${datasetUrlRoot(ApiVersion.v2)}?start=0`, 'builds the base url with a start query');

    // @ts-ignore
    result = datasetsUrl({ platform: 'platform' });
    assert.equal(result, `${datasetUrlRoot(ApiVersion.v2)}/platform/platform?start=0`, 'url includes platform');

    result = datasetsUrl({ platform: 'platform', prefix: 'prefix' });
    assert.equal(
      result,
      `${datasetUrlRoot(ApiVersion.v2)}/platform/platform/prefix/prefix?start=0`,
      'url includes platform'
    );

    // @ts-ignore
    result = datasetsUrl({ prefix: 'prefix' });
    assert.equal(result, `${datasetUrlRoot(ApiVersion.v2)}?start=0`, 'defaults to base url when no platform is passed');
  });
});
