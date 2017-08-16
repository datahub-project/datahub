import { Factory, faker } from 'ember-cli-mirage';
import { fieldIdentifierTypes, idLogicalTypes } from 'wherehows-web/constants';

const identifierTypeValues = Object.keys(fieldIdentifierTypes);

export default Factory.extend({
  fieldName: 'memberUrn',
  identifierTypePrediction: {
    value: () => faker.list.random(...identifierTypeValues),
    confidence: 1
  },
  logicalTypePrediction: {
    value: faker.list.random(...idLogicalTypes),
    confidence: 1
  }
});
