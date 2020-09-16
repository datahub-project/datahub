import { Factory } from 'ember-cli-mirage';
import Faker from 'faker';

const createValidations = (count: number): Com.Linkedin.Common.Health['validations'] =>
  Array.from({ length: count }, () => ({
    score: Math.random(),
    description: Faker.lorem.sentences(),
    weight: Faker.random.number(100),
    tier: Faker.random.arrayElement(['CRITICAL', 'WARNING', 'MINOR']),
    validator: `com.linkedin.metadata.validators${Faker.lorem.word()}Validator`
  }));

export default Factory.extend<Com.Linkedin.Common.Health>({
  score(): number {
    return Math.random();
  },
  validations(): Com.Linkedin.Common.Health['validations'] {
    const numberOfValidations = Faker.random.number({ max: 10, min: 1 });
    return createValidations(numberOfValidations);
  }
});

type EntityHealthWithId = Com.Linkedin.Common.Health & { id: string };

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    entityHealths: EntityHealthWithId;
    entityHealth?: EntityHealthWithId;
  }
}

declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    entityHealths: EntityHealthWithId;
    entityHealth?: EntityHealthWithId;
  }
}
