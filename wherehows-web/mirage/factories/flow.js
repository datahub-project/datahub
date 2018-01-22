import { Factory, faker } from 'ember-cli-mirage';

export default Factory.extend({
  flow_id: faker.random.number({ min: 1000, max: 9000 }),
  appId: faker.random.number({ min: 10, max: 30 }),
  name: '--recreateRootFolder',
  group: 'targeting-expression-evolution',
  level: 0,
  jobCount: 2,
  created: 1371143981901,
  modified: 1381956064561,
  appCode: faker.list.random('AZKABAN PROD WAR', 'LTX1-HOLDEMAZ01')
});
