import { Factory, faker } from 'ember-cli-mirage';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

export default Factory.extend({
  createdTime: faker.date.past(2),
  deprecated: true,
  deprecationNote: faker.lorem.words(5),
  description: faker.lorem.words(7),
  fabric: null,
  modifiedTime: faker.date.recent(),
  nativeType: null,
  platform: faker.list.random(...Object.values(DatasetPlatform)),
  properties: '{}',
  removed: faker.random.boolean(),
  tags: null,
  bucket: null,
  // in search there is a name field
  name(i: number) {
    return `dataset-${i}`;
  },
  nativeName() {
    return this.name;
  },
  uri() {
    const { platform, name, bucket } = this;
    const datasetPath = bucket ? `${bucket}.${name}` : name;
    return `urn:li:dataset:(urn:li:dataPlatform:${platform},${datasetPath},PROD)`;
  },
  urn() {
    return this.uri;
  }
});
