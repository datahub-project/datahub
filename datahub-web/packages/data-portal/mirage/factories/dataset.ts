import { Factory, faker, trait } from 'ember-cli-mirage';
import { testSchemaA } from 'wherehows-web/mirage/fixtures/schema';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

export default Factory.extend({
  id: faker.random.number({ min: 10000, max: 20000 }),
  created: null,
  formatedModified: '2017-09-04 10:34:44.0',
  hasSchemaHistory: false,
  hdfsBrowserLink: null,
  isFavorite: false,
  isOwned: false,
  isWatched: false,
  modified: faker.date.past(),
  properties: () => ({
    DB_ID: faker.random.number({ min: 10000, max: 20000 }),
    TBL_ID: faker.random.number({ min: 10000, max: 20000 }),
    view_depends_on: [faker.commerce.productName()], // eslint-disable-line @typescript-eslint/camelcase
    create_time: faker.date.past(), // eslint-disable-line @typescript-eslint/camelcase
    etl_source: 'COLUMN_V2', // eslint-disable-line @typescript-eslint/camelcase
    input_format: faker.commerce.productName(), // eslint-disable-line @typescript-eslint/camelcase
    output_format: faker.commerce.productName(), // eslint-disable-line @typescript-eslint/camelcase
    is_compressed: false, // eslint-disable-line @typescript-eslint/camelcase
    SD_ID: faker.random.number({ min: 10000, max: 20000 }),
    is_storedassubdirectories: false, // eslint-disable-line @typescript-eslint/camelcase
    serialization_format: 'Sequence', // eslint-disable-line @typescript-eslint/camelcase
    tbl_type: 'VIRTUAL_VIEW', // eslint-disable-line @typescript-eslint/camelcase
    view_expanded_text: faker.lorem.sentence() // eslint-disable-line @typescript-eslint/camelcase
  }),

  // TODO https://github.com/samselikoff/ember-cli-mirage/issues/1379
  whSchema: 'abcd',
  source: faker.random.arrayElement(Object.values(DatasetPlatform)),
  urn: faker.internet.url(),
  name(i: number) {
    return `Dataset ${i}`;
  },
  uri() {
    const platform = this.source;
    const name = this.name;
    return `urn:li:dataset:(urn:li:dataPlatform:${platform},${name},PROD)`;
  },
  watchId: 0,
  owners: () => [],

  forUnitTests: trait({
    id(id: number) {
      return id;
    },

    // TODO https://github.com/samselikoff/ember-cli-mirage/issues/1379
    whSchema(id: number) {
      return id === 0 ? testSchemaA : 'abcd';
    }
  }),

  // TODO https://github.com/samselikoff/ember-cli-mirage/issues/1379
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  afterCreate(dataset: any) {
    dataset.attrs.schema = dataset.attrs.whSchema;
  }
});
