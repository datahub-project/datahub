import { Factory, faker, trait } from 'ember-cli-mirage';
import { IHealthScoreObject } from 'wherehows-web/typings/api/datasets/health';

const tierOptions = ['CRITICAL', 'WARNING', 'MINOR'];

export default Factory.extend({
  id(id: number) {
    return id;
  },
  score: faker.random.number({ min: 0, max: 100 }),
  validations() {
    const numValidations = faker.random.number({ min: 1, max: 6 });
    const validations: Array<IHealthScoreObject> = [];

    for (let i = 0; i < numValidations; i++) {
      const validation: IHealthScoreObject = {
        tier: tierOptions[i % 3],
        score: faker.random.number({ min: 0, max: 100 }) / 100,
        description: faker.lorem.sentences(),
        weight: faker.random.number({ min: 0, max: 100 }) / 100,
        validator: 'fake'
      };

      validations.push(validation);
    }

    return validations;
  },

  refDatasetUrn() {
    return this.dataset ? this.dataset.urn : 'fakeUrn';
  },

  forTesting: trait({
    score: 83,
    validations() {
      const validations: Array<IHealthScoreObject> = [];

      for (let i = 0; i < 3; i++) {
        const validation: IHealthScoreObject = {
          tier: tierOptions[i],
          score: 1 - (3 - i) * 0.25,
          description: faker.lorem.sentences(2),
          weight: faker.random.number({ min: 0, max: 100 }) / 100,
          validator: 'fake'
        };

        validations.push(validation);
      }
    }
  })
});
