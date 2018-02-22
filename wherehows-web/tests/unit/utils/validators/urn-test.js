import { module, test } from 'qunit';
import {
  convertWhDatasetPathToLiPath,
  datasetUrnRegexWH,
  convertWhUrnToLiUrn
} from 'wherehows-web/utils/validators/urn';
import { whUrnToLiUrnMap } from 'wherehows-web/mirage/fixtures/urn';

module('Unit | Utility | validators/urn');

test('converters exist', function(assert) {
  assert.ok(typeof convertWhDatasetPathToLiPath === 'function', 'convertWhDatasetPathToLiPath is a function');
  assert.ok(typeof convertWhUrnToLiUrn === 'function', 'convertWhUrnToLiUrn is a function');
});

test('convertWhDatasetPathToLiPath correctly converts an hdfs path', function(assert) {
  const [, platform, path] = datasetUrnRegexWH.exec('hdfs:///seg1/seg2/seg3/data/kebab-db-name');
  const result = convertWhDatasetPathToLiPath(platform, path);

  assert.equal('/seg1/seg2/seg3/data/kebab-db-name', result, 'hdfs path is correctly converted');
});

test('convertWhDatasetPathToLiPath correctly converts a non-hdfs path', function(assert) {
  const [, platform, path] = datasetUrnRegexWH.exec('oracle:///ABOOK/ABOOK_DATA');
  const result = convertWhDatasetPathToLiPath(platform, path);

  assert.equal('ABOOK.ABOOK_DATA', result, 'non hdfs path is correctly converted');
});

test('convertWhUrnToLiUrn correctly converts urns', function(assert) {
  assert.expect(whUrnToLiUrnMap.length);

  whUrnToLiUrnMap.forEach(([actual, expected]) =>
    assert.equal(convertWhUrnToLiUrn(actual), expected, `${actual} is correctly converted to ${expected}`)
  );
});
