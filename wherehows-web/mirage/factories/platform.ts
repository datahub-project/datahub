import { Factory, faker } from 'ember-cli-mirage';
import { DatasetPlatform, PurgePolicy } from 'wherehows-web/constants';

export default Factory.extend({
  name: faker.list.random(...Object.values(DatasetPlatform)),

  type: faker.lorem.words(1),

  supportedPurgePolicies: Object.values(PurgePolicy)
});
