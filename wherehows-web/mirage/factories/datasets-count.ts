import { Factory, faker } from 'ember-cli-mirage';

export default Factory.extend({
  count() {
    return faker.random.number({ min: 0, max: 10000 });
  }
});
