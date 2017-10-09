import { Factory, faker, trait } from 'ember-cli-mirage';

export default Factory.extend({
  created() { return null; },
  formatedModified() { return "2017-09-04 10:34:44.0"; },
  hasSchemaHistory() { return false; },
  hdfsBrowserLink() { return null; },
  isFavorite() { return false; },
  isOwned() { return false; },
  isWatched() { return false; },
  modified() { return faker.date.past(); },
  name() { return faker.commerce.productName(); },
  properties() { return {
    DB_ID: faker.random.number({ min: 10000, max: 20000 }),
    TBL_ID: faker.random.number({ min: 10000, max: 20000 }),
    view_depends_on:[
       faker.commerce.productName()
    ],
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
  }; },
  schema() { return "abcd"; },
  source() { return 'Hive'; },
  urn() { return faker.internet.url(); },
  watchId() { return 0; },
  owners() { return []; },
});
