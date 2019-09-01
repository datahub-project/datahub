import {
  isDatasetIdentifier,
  isDatasetPlatform,
  isDatasetSegment
} from '@datahub/data-models/entity/dataset/utils/platform';
import { module, test } from 'qunit';

module('Unit | Utility | validators/platform', function() {
  const platform = '[platform=aPlatform]';
  const segmentA = '/a/db/segment/';
  const segmentB = 'a/db/segment/';
  const segmentC = 'a.db.segment.';
  const segmentD = '/.';

  test('isDatasetIdentifier', function(assert) {
    let result = isDatasetIdentifier();
    assert.ok(result === false, 'undefined is not a dataset identifier');

    result = isDatasetIdentifier(null);
    assert.ok(result === false, 'null is not a dataset identifier');

    [platform, segmentA, segmentB, segmentC, segmentD].forEach(test =>
      assert.ok(isDatasetIdentifier(test) === false, `${test} is not a dataset identifier`)
    );
  });

  test('isDatasetPlatform', function(assert) {
    let result = isDatasetPlatform(platform);
    assert.ok(result === true, `${platform} is a dataset platform`);

    result = isDatasetPlatform();
    assert.ok(result === false, `undefined is not a dataset platform`);
  });

  test('isDatasetSegment', function(assert) {
    [segmentA, segmentB, segmentC, segmentD].forEach(test =>
      assert.ok(isDatasetSegment(test) === true, `${test} is a dataset segment`)
    );
  });
});
