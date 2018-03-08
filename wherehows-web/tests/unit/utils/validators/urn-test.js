import { module, test } from 'qunit';
import {
  convertWhDatasetPathToLiPath,
  datasetUrnRegexWH,
  convertWhUrnToLiUrn,
  buildLiUrn,
  isLiUrn
} from 'wherehows-web/utils/validators/urn';
import { whUrnToLiUrnMap } from 'wherehows-web/mirage/fixtures/urn';
import { DatasetPlatform } from 'wherehows-web/constants';

const hdfsPath = '/seg1/seg2/seg3/data/kebab-db-name';
const dbPath = 'ABOOK/ABOOK_DATA';

module('Unit | Utility | validators/urn');

test('converters exist', function(assert) {
  assert.ok(typeof convertWhDatasetPathToLiPath === 'function', 'convertWhDatasetPathToLiPath is a function');
  assert.ok(typeof convertWhUrnToLiUrn === 'function', 'convertWhUrnToLiUrn is a function');
});

test('convertWhDatasetPathToLiPath correctly converts an hdfs path', function(assert) {
  const [, platform, path] = datasetUrnRegexWH.exec(`hdfs:///${hdfsPath.slice(1)}`);
  const result = convertWhDatasetPathToLiPath(platform, path);

  assert.equal(hdfsPath, result, 'hdfs path is correctly converted');
});

test('convertWhDatasetPathToLiPath correctly converts a non-hdfs path', function(assert) {
  const [, platform, path] = datasetUrnRegexWH.exec(`oracle:///${dbPath}`);
  const result = convertWhDatasetPathToLiPath(platform, path);

  assert.equal('ABOOK.ABOOK_DATA', result, 'non hdfs path is correctly converted');
});

test('convertWhUrnToLiUrn correctly converts urns', function(assert) {
  assert.expect(whUrnToLiUrnMap.length);

  whUrnToLiUrnMap.forEach(([actual, expected]) =>
    assert.equal(convertWhUrnToLiUrn(actual), expected, `${actual} is correctly converted to ${expected}`)
  );
});

test('buildLiUrn', function(assert) {
  let result = buildLiUrn(DatasetPlatform.HDFS, '');
  assert.ok(isLiUrn(result), `creates a valid li urn with platform ${DatasetPlatform.HDFS} and empty path`);

  result = buildLiUrn(DatasetPlatform.MySql, dbPath);
  assert.ok(isLiUrn(result), `creates a valid li urn with platform ${DatasetPlatform.MySql} and db path`);

  result = buildLiUrn(DatasetPlatform.HDFS, '/seg1/seg2/seg3/data/kebab-db-name');
  assert.ok(
    isLiUrn(result),
    `creates a valid li urn with platform ${DatasetPlatform.HDFS} and path with forward slashes`
  );

  result = buildLiUrn(DatasetPlatform.KafkaLc, dbPath, 'PROD');
  assert.ok(isLiUrn(result), `creates a valid li urn with platform ${DatasetPlatform.KafkaLc}, db path and fabric`);
});
