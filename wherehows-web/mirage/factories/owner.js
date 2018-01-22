import { Factory, faker, association } from 'ember-cli-mirage';

export default Factory.extend({
  userName() {
    return faker.internet.userName();
  },
  departmentNum() {
    return 0;
  },
  email() {
    return faker.internet.email();
  },
  name() {
    return faker.name.firstName() + ' ' + faker.name.lastName();
  },
  userSetting() {
    return null;
  },
  flowId() {
    return faker.random.number({ min: 1, max: 10 });
  }
});
