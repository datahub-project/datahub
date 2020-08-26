import { Factory, faker } from 'ember-cli-mirage';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';

export default Factory.extend({
  datasetId: null,

  datasetUrn: 'fake_pika',

  purgeType: faker.list.random(...Object.values(PurgePolicy)),

  purgeNote(): string {
    const { purgeType } = this;
    return purgeType === PurgePolicy.PurgeExempt ? faker.lorem.words(5) : '';
  },

  modifiedBy: faker.internet.userName(),

  modifiedTime: faker.date.past()
});
