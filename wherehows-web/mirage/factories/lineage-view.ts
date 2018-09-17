import { Factory, faker } from 'ember-cli-mirage';
import { DatasetPlatform } from 'wherehows-web/constants';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

export default Factory.extend({
  dataset() {
    const platform = faker.list.random(...Object.values(DatasetPlatform));

    return {
      createdTime: faker.date.past(2),
      deprecated: true,
      deprecationNote: faker.lorem.words(5),
      description: faker.lorem.words(7),
      fabric: null,
      modifiedTime: faker.date.recent(),
      nativeName: 'abook.default-public-container',
      nativeType: null,
      platform,
      properties: '{}',
      removed: faker.random.boolean(),
      tags: null,
      uri:
        platform === DatasetPlatform.HDFS
          ? hdfsUrn
          : nonHdfsUrn.replace(/li:dataPlatform:db/, `li:dataPlatform:${platform}`)
    };
  },

  actor: 'corpuser:lskywalker',

  type: 'fake-type'
});
