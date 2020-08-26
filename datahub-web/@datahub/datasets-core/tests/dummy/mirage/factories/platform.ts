import { Factory, faker } from 'ember-cli-mirage';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';

const platforms = Object.values(DatasetPlatform);

/**
 * Some datasets have slash in their name as prefix as a map for easy exist check
 */
const PlatformsWithSlash: Partial<Record<DatasetPlatform, true>> = {
  [DatasetPlatform.HDFS]: true,
  [DatasetPlatform.SEAS_HDFS]: true,
  [DatasetPlatform.SEAS_DEPLOYED]: true
};

export default Factory.extend({
  name(id: number): DatasetPlatform {
    return platforms[id];
  },

  type: faker.lorem.words(1),

  supportedPurgePolicies: Object.values(PurgePolicy),

  datasetNameDelimiter(): string {
    return PlatformsWithSlash[this.name as DatasetPlatform] ? '/' : '.';
  }
});
