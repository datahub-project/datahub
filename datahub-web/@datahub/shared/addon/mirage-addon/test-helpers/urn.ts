import { decodeUrn } from '@datahub/utils/validators/urn';

const urn = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,%2Fseg1s%2Fseg2%2Fseg3%2Fseg4%2Fdataset-node,PROD)';
const nonHdfsUrn = 'urn:li:dataset:(urn:li:dataPlatform:db,ABOOK.ABOOK_DATA,PROD)';
const hdfsUrn = decodeUrn(urn);

const whUrnToLiUrnMap = [
  ['espresso:///ETLInfra/AllTables', 'urn:li:dataset:(urn:li:dataPlatform:espresso,ETLInfra.AllTables,PROD)'],
  [
    'hdfs:///seg1/seg2/seg3/data/kebab-db-name',
    'urn:li:dataset:(urn:li:dataPlatform:hdfs,/seg1/seg2/seg3/data/kebab-db-name,PROD)'
  ],
  [
    'hdfs:///seg1/seg2/data/UpperCaseDbName',
    'urn:li:dataset:(urn:li:dataPlatform:hdfs,/seg1/seg2/data/UpperCaseDbName,PROD)'
  ],
  ['oracle:///ABOOK/ABOOK_DATA', 'urn:li:dataset:(urn:li:dataPlatform:oracle,ABOOK.ABOOK_DATA,PROD)']
];

const liUrns = [
  'urn:li:dataset:(urn:li:dataPlatform:oracle,ABOOK.ABOOK_EMAILS,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:gibberish,gibberish.gibberish,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:hdfs,/gibberish/gibberish/gIbberish/gibberish/gibberish-gibberish-gibberish,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:gibberish,GIBBERISH.GIBBERISH_GIBBERISH,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:gibberish-gibberish,GIBBERISH.gibberish-gibberish-com.gibberish.gibberish.gibberish.Windows-gibberish,PROD)'
];

export { urn, whUrnToLiUrnMap, hdfsUrn, nonHdfsUrn, liUrns };
