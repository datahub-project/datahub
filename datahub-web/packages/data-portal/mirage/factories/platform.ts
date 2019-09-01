import { Factory, faker } from 'ember-cli-mirage';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { PlatformsWithSlash } from '@datahub/data-models/entity/dataset/utils/urn';

const platforms = Object.values(DatasetPlatform);

export default Factory.extend({
  name(id: number) {
    return platforms[id];
  },

  type: faker.lorem.words(1),

  datasetNameDelimiter() {
    return PlatformsWithSlash[this.name as DatasetPlatform] ? '/' : '.';
  }
});
