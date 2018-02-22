const urn = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,%2Fseg1s%2Fseg2%2Fseg3%2Fseg4%2Fdataset-node,PROD)';

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

export { urn, whUrnToLiUrnMap };
