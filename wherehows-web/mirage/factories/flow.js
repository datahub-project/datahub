import { Factory, faker } from 'ember-cli-mirage';

export default Factory.extend({
  flow_id() { return faker.random.number({ min: 1000, max: 9000 }); },
  appId() { return faker.random.number({ min: 10, max: 30 }); },
  name() { return '--recreateRootFolder'; },
  group() { return 'targeting-expression-evolution'; },
  level() { return 0; },
  jobCount() { return 2; },
  created() { return 1371143981901; },
  modified() { return 1381956064561; },
  appCode() { return faker.list.random('AZKABAN PROD WAR' ,'LTX1-HOLDEMAZ01'); }
});
