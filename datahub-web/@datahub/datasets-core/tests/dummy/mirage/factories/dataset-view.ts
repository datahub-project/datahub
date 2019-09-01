import { Factory, faker } from 'ember-cli-mirage';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { capitalize } from '@ember/string';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

export default Factory.extend({
  createdTime: 1552521600 * 1000,
  decommissionTime: null,
  deprecated: false,
  deprecationNote: '',
  description: 'A dataset generated from pallet town',
  fabric: faker.list.cycle(FabricType.CORP, FabricType.PROD, FabricType.EI),
  modifiedTime() {
    return this.createdTime + 24 * 60 * 60 * 5 * 1000;
  },
  nativeName: faker.random.word() + capitalize(faker.random.word()),
  nativeType: '',
  platform: DatasetPlatform.HDFS,
  properties: '',
  removed: false,
  tags(): Array<string> {
    return [];
  },
  uri: 'urn'
});
