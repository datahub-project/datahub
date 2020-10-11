import { Factory, faker } from 'ember-cli-mirage';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';

export default Factory.extend<IDatasetSchemaColumn>({
  comment: faker.lorem.sentence,
  //faker 3 does not have database
  dataType: (faker.database && faker.database.type) || ((i: number): string => `data_type_${i}`),
  distributed: faker.random.boolean,
  //faker 3 does not have database
  fieldName: (faker.database && faker.database.column) || ((i: number): string => `field_name_${i}`),
  fullFieldPath: faker.system.filePath,
  indexed: faker.random.boolean,
  nullable: faker.random.boolean,
  partitioned: faker.random.boolean,
  treeGridClass: null,
  commentCount(): number {
    return faker.random.number(1000);
  },
  id(): number | null {
    return faker.random.arrayElement([faker.random.number(), null]);
  },
  parentSortID() {
    return faker.random.number(100);
  },
  sortID(): number {
    return faker.random.number(100);
  }
});

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    datasetSchemaColumns: IDatasetSchemaColumn;
    datasetSchemaColumn?: IDatasetSchemaColumn;
  }
}

declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    datasetSchemaColumns: IDatasetSchemaColumn;
    datasetSchemaColumn?: IDatasetSchemaColumn;
  }
}
