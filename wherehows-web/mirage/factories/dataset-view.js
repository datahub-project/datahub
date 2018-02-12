import { Factory, faker } from 'ember-cli-mirage';

export default Factory.extend({
  createdTime: faker.date.past(2),
  deprecated: true,
  deprecationNote: faker.lorem.words(5),
  description: faker.lorem.words(7),
  fabric: null,
  modifiedTime: faker.date.recent(),
  nativeName: 'abook.default-public-container',
  nativeType: null,
  platform: 'ambry',
  properties: '{}',
  removed: faker.random.boolean(),
  tags: null,
  uri: 'ambry:///abook.default-public-container'
});
