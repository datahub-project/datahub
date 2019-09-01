import { module, test } from 'qunit';
import {
  convertWhDatasetPathToLiPath,
  datasetUrnRegexWH,
  datasetUrnRegexLI,
  convertWhUrnToLiUrn,
  buildDatasetLiUrn,
  isLiUrn,
  getDatasetUrnParts
} from '@datahub/data-models/entity/dataset/utils/urn';
import { whUrnToLiUrnMap, liUrns } from 'wherehows-web/mirage/fixtures/urn';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';

const hdfsPath = '/seg1/seg2/seg3/data/kebab-db-name';
const dbPath = 'ABOOK/ABOOK_DATA';

const urnParseTests = [
  {
    urn: 'urn:li:dataset:(urn:li:dataPlatform:mysql,harrier.page,CORP)',
    expected: {
      platform: 'mysql' as DatasetPlatform,
      fabric: 'CORP' as FabricType,
      prefix: 'harrier.page'
    }
  },
  {
    urn: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,/user/xdai/u_xdai.db/page,PROD)',
    expected: {
      platform: 'hdfs' as DatasetPlatform,
      fabric: 'PROD' as FabricType,
      prefix: '/user/xdai/u_xdai.db/page'
    }
  }
];

module('Unit | Utility | validators/urn', function() {
  test('converters exist', function(assert) {
    assert.ok(typeof convertWhDatasetPathToLiPath === 'function', 'convertWhDatasetPathToLiPath is a function');
    assert.ok(typeof convertWhUrnToLiUrn === 'function', 'convertWhUrnToLiUrn is a function');
  });

  test('datasetUrnRegexLI', function(assert) {
    assert.expect(liUrns.length);
    liUrns.forEach(liUrn => assert.ok(datasetUrnRegexLI.test(liUrn), `${liUrn} is a valid urn`));
  });

  test('convertWhDatasetPathToLiPath correctly converts an hdfs path', function(assert) {
    const [, platform = '', path = ''] = datasetUrnRegexWH.exec(`hdfs:///${hdfsPath.slice(1)}`) || [];
    const result = convertWhDatasetPathToLiPath(platform as DatasetPlatform, path);

    assert.equal(hdfsPath, result, 'hdfs path is correctly converted');
  });

  test('convertWhDatasetPathToLiPath correctly converts a non-hdfs path', function(assert) {
    const [, platform = '', path = ''] = datasetUrnRegexWH.exec(`oracle:///${dbPath}`) || [];
    const result = convertWhDatasetPathToLiPath(platform as DatasetPlatform, path);

    assert.equal('ABOOK.ABOOK_DATA', result, 'non hdfs path is correctly converted');
  });

  test('convertWhUrnToLiUrn correctly converts urns', function(assert) {
    assert.expect(whUrnToLiUrnMap.length);

    whUrnToLiUrnMap.forEach(([actual, expected]) =>
      assert.equal(convertWhUrnToLiUrn(actual), expected, `${actual} is correctly converted to ${expected}`)
    );
  });

  test('buildLiUrn', function(assert) {
    let result = buildDatasetLiUrn(DatasetPlatform.HDFS, '');
    assert.ok(isLiUrn(result), `creates a valid li urn with platform ${DatasetPlatform.HDFS} and empty path`);

    result = buildDatasetLiUrn(DatasetPlatform.MySql, dbPath);
    assert.ok(isLiUrn(result), `creates a valid li urn with platform ${DatasetPlatform.MySql} and db path`);

    result = buildDatasetLiUrn(DatasetPlatform.HDFS, '/seg1/seg2/seg3/data/kebab-db-name');
    assert.ok(
      isLiUrn(result),
      `creates a valid li urn with platform ${DatasetPlatform.HDFS} and path with forward slashes`
    );

    result = buildDatasetLiUrn(DatasetPlatform.KafkaLc, dbPath, FabricType.PROD);
    assert.ok(isLiUrn(result), `creates a valid li urn with platform ${DatasetPlatform.KafkaLc}, db path and fabric`);
  });

  test('getDatasetUrnParts', function(assert) {
    urnParseTests.forEach(parseTest => {
      const result = getDatasetUrnParts(parseTest.urn);
      assert.deepEqual(result, parseTest.expected);
    });
  });
});
