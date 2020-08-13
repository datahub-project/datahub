import { Factory, faker, trait } from 'ember-cli-mirage';

const NONE_IDENTIFIER = 'NONE';

/**
 * A list of static identifier fields to cycle through for testing purposes. This is to help reduce the variation
 * in identifier fields to a list of expected values
 */
const identifierFields = [
  'CONTACT_ID[type = long]',
  'CONTACT_ID[type = long]',
  'DATA_XML_VERSION[type = long]',
  'DATA[type = string]',
  'DELETED_TS[type = long]',
  'GG_MODI_TS[type = long]',
  'GG_STATUS[type = string]',
  'IS_NOTE_MANUALLY_MOD[type = string]',
  'lumos_dropdate',
  'MODIFIED_DATE'
];

/**
 * Maps some identifier types to their corresponding fields above. If not included here, we assume the identifier
 * type to then be "NONE"
 */
const identifierTypeMap: Record<string, Array<string>> = {
  MEMBER_ID: ['CONTACT_ID[type = long]', 'DATA_XML_VERSION[type = long]'],
  GROUP_ID: ['CONTACT_ID[type = long]'],
  COMPANY_ID: ['DATA[type = string]']
};

const identifierTypeSuggestionMap: Record<string, Array<string>> = {
  COMPANY_ID: ['CONTACT_ID[type = long]', 'DATA_XML_VERSION[type = long]', 'DATA[type = string]'],
  GROUP_ID: ['DATA_XML_VERSION[type = long]'],
  ADVERTISER_ID: ['GG_STATUS[type = string]', 'CONTACT_ID[type = long]']
};

/**
 * Since the above is a map, this creates a list of possible types aside from "NONE"
 */
const identifierTypes = Object.keys(identifierTypeMap);
const suggestionIdentifierTypes = Object.keys(identifierTypeSuggestionMap);

/**
 * Note: The dataset compliance annotation tag is found in the datasets entity page on the "Compliance" tab and represents
 * a "tag" for a single field in the dataset to represent whether or not it has PII and, if so, further details about the
 * type of data the field contains
 */
export default Factory.extend({
  identifierField: faker.list.cycle(...identifierFields),

  identifierType(id: number): string {
    const field = this.identifierField as string;
    // This logic determines identifier type by searching through the map to see if there is an available type
    // being mapped, otherwise defaults to NONE, but in multi tagging situations we'll need to have a way to
    // determine which tag to use, hence the even/odd logic
    return identifierTypes.reduce(
      (identifierType, currentType) =>
        identifierTypeMap[currentType].indexOf(field) > -1
          ? identifierType !== NONE_IDENTIFIER && id % 2 === 0
            ? identifierType
            : currentType
          : identifierType,
      NONE_IDENTIFIER
    );
  },

  logicalType(): string | null {
    return this.identifierType === NONE_IDENTIFIER ? null : 'URN';
  },
  // If none identifier, we automatically know there is no purge key
  nonOwner(): boolean {
    return this.identifierType === NONE_IDENTIFIER || Math.random() > 0.5;
  },

  pii(): boolean {
    return this.identifierType === 'MEMBER_ID';
  },

  readonly: false,

  securityClassification(): string | null {
    return this.identifierType === NONE_IDENTIFIER ? null : 'LIMITED_DISTRIBUTION';
  },

  valuePattern: null,

  notPurgeKey: trait({
    nonOwner: true
  }),

  withPurgeKey: trait({
    nonOwner: false
  }),

  isSuggestion: false,

  asSuggestion: trait({
    identifierField: faker.list.cycle(...identifierFields),
    identifierType(id: number): string {
      const field = this.identifierField;
      // This logic determines identifier type by searching through the map to see if there is an available type
      // being mapped, otherwise defaults to NONE, but in multi tagging situations we'll need to have a way to
      // determine which tag to use, hence the even/odd logic
      return suggestionIdentifierTypes.reduce(
        (identifierType, currentType) =>
          identifierTypeSuggestionMap[currentType].indexOf(field) > -1
            ? identifierType !== NONE_IDENTIFIER && id % 2 === 0
              ? identifierType
              : currentType
            : identifierType,
        NONE_IDENTIFIER
      );
    },
    isSuggestion: true
  })
});
