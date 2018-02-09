import { Factory, faker } from 'ember-cli-mirage';

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
  name: faker.commerce.productName(),
  properties: () => ({
    DB_ID: faker.random.number({ min: 10000, max: 20000 }),
    TBL_ID: faker.random.number({ min: 10000, max: 20000 }),
    view_depends_on: [faker.commerce.productName()],
    create_time: faker.date.past(),
    etl_source: 'COLUMN_V2',
    input_format: faker.commerce.productName(),
    output_format: faker.commerce.productName(),
    is_compressed: false,
    SD_ID: faker.random.number({ min: 10000, max: 20000 }),
    is_storedassubdirectories: false,
    serialization_format: 'Sequence',
    tbl_type: 'VIRTUAL_VIEW',
    view_expanded_text: faker.lorem.sentence()
  }),
  schema: 'abcd',
  source: 'Hive',
  urn: faker.internet.url(),
  watchId: 0,
  owners: () => []
});
