import { Factory, faker } from 'ember-cli-mirage';
import { PurgePolicy } from 'wherehows-web/constants';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

export default Factory.extend({
  datasetId: null,

  datasetUrn: faker.list.random(hdfsUrn, nonHdfsUrn),

  purgeType: faker.list.random(...Object.values(PurgePolicy)),

  purgeNote() {
    const { purgeType } = this;
    return purgeType === PurgePolicy.PurgeExempt ? faker.lorem.words(5) : '';
  },

  modifiedBy: faker.internet.userName(),

  modifiedTime: faker.date.past()
});
