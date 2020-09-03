import { Factory } from 'ember-cli-mirage';
import faker from 'faker';
import { IOwnerResponse, IOwner } from '@datahub/data-models/types/entity/dataset/ownership';
import { corpUserUrnBasePrefix } from '@datahub/data-models/config/urn/base-prefix';
import { OwnerUrnNamespace } from '@datahub/data-models/constants/entity/dataset/ownership';

export default Factory.extend<IOwnerResponse>({
  owners(): Array<IOwner> {
    const numberOfOwners = faker.random.number({ min: 1, max: 10 });
    return Array.from(
      { length: numberOfOwners },
      (): IOwner => ({
        confirmedBy: null,
        email: faker.internet.email(),
        isActive: faker.random.arrayElement([true, false]),
        isGroup: faker.random.arrayElement([true, false]),
        name: faker.name.firstName(),
        modifiedTime: faker.date.recent().getTime(),
        idType: faker.random.arrayElement(['USER', 'GROUP', 'SERVICE']),
        namespace: faker.random.arrayElement(Object.values(OwnerUrnNamespace)),
        source: faker.random.arrayElement([
          'AUDIT',
          'DATA_VAULT',
          'DATABASE',
          'FILE_SYSTEM',
          'JIRA',
          'NUAGE',
          'OTHER',
          'REVIEW_BOARD',
          'SOS',
          'SOURCE_CONTROL',
          'USER_INTERFACE'
        ]),
        type: faker.random.arrayElement(['DATA_OWNER', 'PRODUCER', 'DELEGATE', 'STAKEHOLDER', 'CONSUMER']),
        userName: faker.internet.userName(),
        sortId: null,
        subType: null
      })
    );
  },
  fromUpstream(): boolean {
    return faker.random.arrayElement([true, false]);
  },
  datasetUrn: faker.lorem.slug,
  lastModified(): number {
    return faker.date.recent().getTime();
  },
  actor(): string {
    return `${corpUserUrnBasePrefix}${faker.internet.userName()}`;
  }
});

// IOwnerResponse augmented with a urn to support Mirage DB request that use the requested urn to find an instance
type OwnerResponseWithUrn = IOwnerResponse & { urn: string };

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    datasetOwnerships: OwnerResponseWithUrn;
    datasetOwnership?: OwnerResponseWithUrn;
  }
}
declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    datasetOwnerships: OwnerResponseWithUrn;
    datasetOwnership?: OwnerResponseWithUrn;
  }
}
