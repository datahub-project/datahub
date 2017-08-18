import { Factory, faker } from 'ember-cli-mirage';
import {
  fieldIdentifierTypes,
  idLogicalTypes,
  customIdLogicalTypes,
  nonIdFieldLogicalTypes
} from 'wherehows-web/constants';

const { idIds, nonIdIds } = Object.keys(fieldIdentifierTypes).reduce(
  ({ idIds, nonIdIds }, fieldIdTypeKey) => {
    const fieldIdentifier = fieldIdentifierTypes[fieldIdTypeKey].value;
    if (fieldIdentifierTypes[fieldIdTypeKey].isId) {
      idIds = [...idIds, fieldIdentifier];
    } else {
      nonIdIds = [...nonIdIds, fieldIdentifier];
    }

    return { idIds, nonIdIds };
  },
  { idIds: [], nonIdIds: [] }
);

export default Factory.extend({
  fieldName: "address[type='com.linkedin.common.MultiLocaleString']",

  identifierTypePrediction: {
    value: (i) => {
      const fieldIdentifierIsId = i % 2;
      return faker.list.random(...(fieldIdentifierIsId ? idIds : nonIdIds))();
    },
    confidence: Math.random()
  },

  logicalTypePrediction: {
    value: (i) => {
      const fieldIdentifierIsId = i % 2;
      const idArray = [...idLogicalTypes, ...customIdLogicalTypes];
      return faker.list.random(...(fieldIdentifierIsId ? idArray : Object.keys(nonIdFieldLogicalTypes)))();
    },
    confidence: Math.random()
  }
});
