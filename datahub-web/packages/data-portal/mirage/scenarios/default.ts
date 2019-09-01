import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

const fixtures = [
  'dataset-nodes',
  'metric-metrics',
  'user-entities',
  'compliance-data-types',
  'list-platforms',
  'dataset-acl-users',
  'browse-platforms',
  'search-response'
];

export default function(server: IMirageServer): void {
  server.loadFixtures(...fixtures);
  server.create('config');
  server.createList('owner', 6);
  server.createList('dataset', 10);
  // datasetView type UMP and datasetUmp needs to be together to create same Ids
  server.createList('datasetView', 10, {
    platform: DatasetPlatform.UMP,
    removed: false,
    deprecated: false,
    bucket: 'careers'
  });
  server.createList('datasetOwnership', 3, { datasetId: 'dataset-0' });
  server.createList('datasetView', 10, { platform: DatasetPlatform.Ambry, removed: false, deprecated: false });
  server.createList('datasetView', 2, { platform: DatasetPlatform.Hive });
  server.createList('datasetView', 1, {
    platform: DatasetPlatform.HDFS,
    removed: false,
    deprecated: false,
    name: '/some/path/with/directories/adataset1'
  });
  server.createList('datasetView', 1, {
    platform: DatasetPlatform.HDFS,
    removed: false,
    deprecated: false,
    name: '/some/path/with/directories/adataset2'
  });
  server.createList('datasetView', 1, {
    platform: DatasetPlatform.HDFS,
    removed: false,
    deprecated: false,
    name: '/some/path/with/otherdir/adataset3'
  });

  server.createList('column', 2);
  server.createList('comment', 2);
  server.createList('depend', 2);
  server.createList('impact', 2);
  server.createList('instance', 2);
  server.createList('ownerType', 2);
  server.createList('reference', 2);
  server.createList('sample', 2);
  server.createList('suggestion', 2);
  server.createList('platform', Object.values(DatasetPlatform).length);
  server.createList('version', 2);
}
