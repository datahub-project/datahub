import { Factory, faker } from 'ember-cli-mirage';

export default Factory.extend({
  datasetId(i: number) {
    return `dataset-${i}`;
  },
  owner: faker.random.arrayElement(['urn:li:corpuser:jywadhwa', 'urn:li:corpuser:catran', 'urn:li:corpuser:ibonapie']),
  onwerCategory: faker.random.arrayElement(['CONSUMER']),
  ownershipProvider: faker.random.arrayElement(['FILE_SYSTEM', 'JIRA']),
  sourceUrl: faker.random.arrayElement(['someSourceUrl', 'anotherSourceUrl']) //optional
});
