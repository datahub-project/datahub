import { Factory, faker } from 'ember-cli-mirage';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

// URN is needed for search results
export default Factory.extend<Com.Linkedin.Dataset.Dataset & { urn: string }>({
  id(i: number): number {
    return i;
  },
  description: faker.lorem.paragraph(1),
  platformNativeType: faker.random.arrayElement(['TABLE', 'VIEW', 'DIRECTORY', 'STREAM', 'BUCKET']),
  removed: false,
  name(i: number): string {
    return `dataset-${i}`;
  },
  platform: faker.random.arrayElement(
    // TODO: [META-11778] This should be no longer necessary once we provide a more elegant solution
    // We remove the UMP platform from the random chooser as this can create issues when the
    // platform is not expected to be UMP
    Object.values(DatasetPlatform).filter((platform): boolean => platform !== DatasetPlatform.UMP)
  ),
  origin: 'PROD', //defaults to PROD
  uri(): string {
    return `urn:li:dataset:(urn:li:dataPlatform:${this.platform},${this.name},${this.origin})`;
  },
  urn(): string {
    return this.uri as string;
  },
  deploymentInfos: () => [],
  tags: () => [],
  created: () => ({
    actor: faker.name.firstName(),
    time: faker.date.past().getTime()
  }),
  lastModified: () => ({
    actor: faker.name.firstName(),
    time: faker.date.past().getTime()
  }),
  health: () => ({
    validations: [],
    score: Math.random()
  })
});

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    datasets: Com.Linkedin.Dataset.Dataset & { urn: string };
    dataset?: Com.Linkedin.Dataset.Dataset & { urn: string };
  }
}

declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    datasets: Com.Linkedin.Dataset.Dataset & { urn: string };
    dataset?: Com.Linkedin.Dataset.Dataset & { urn: string };
  }
}
