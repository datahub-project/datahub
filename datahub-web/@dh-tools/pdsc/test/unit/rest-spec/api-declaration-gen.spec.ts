import chai = require('chai');
import * as findRoot from 'find-root';
import * as fs from 'fs';
import * as path from 'path';
import { generateTs, loadRestSpec } from '../../../src/rest-spec/api-declaration-gen';

const assert = chai.assert;

const fixtures = path.join(findRoot(), 'test/fixtures/rest-spec/api-rest-model-0.1.21');

describe('generateTs', () => {
  it('parses', () => {
    const project = findRoot();
    const fixture = path.join(fixtures, 'com.linkedin.sailfish.feed.sailfishFeedUpdates.restspec.json');
    const expected = fs.readFileSync(path.join(fixtures, 'expected.ts.txt')).toString();
    const restSpec = loadRestSpec(fixture);
    const ts = generateTs(restSpec, {
      ignoreNamespace: 'com.',
      collectionType: 'Custom.CollectionResult'
    });
    assert.equal(expected, ts);
  });
});
