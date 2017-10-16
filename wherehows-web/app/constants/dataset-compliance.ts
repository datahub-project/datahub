import { fieldIdentifierTypes } from 'wherehows-web/constants/datasets/compliance';
/**
 * Defines a map of values for the compliance policy on a dataset
 * @type {object}
 */
const compliancePolicyStrings = {
  // TODO:  DSS-6122 Create and move to Error module
  complianceDataException: 'Unexpected discrepancy in compliance data',
  missingTypes: 'Looks like you may have forgotten to specify a `Field Format` for all ID fields?',
  successUpdating: 'Congrats! Your changes have been successfully saved!',
  failedUpdating: 'Oops! We are having trouble updating this dataset at the moment.',
  successUploading: 'Metadata successfully updated! Please "Save" when ready.',
  invalidPolicyData: 'Received policy in an unexpected format! Please check the provided attributes and try again.',
  helpText: {
    classification:
      'The default value is taken from go/dht and should be good enough in most cases. ' +
      'You can optionally override it if required by house security.'
  }
};

/**
 * List of identifier type keys without the none object value
 * @type {Array<string>}
 */
const fieldIdentifierTypeKeysBarNone: Array<string> = Object.keys(fieldIdentifierTypes).filter(k => k !== 'none');

/**
 * Keys for the field display options
 * @type {Array<string>}
 */
const fieldDisplayKeys: Array<string> = ['none', '_', ...fieldIdentifierTypeKeysBarNone];

/**
 * A list of field identifier types mapped to label, value options for select display
 * @type {Array<{value: string, label: string, isDisabled: boolean}>}
 */
const fieldIdentifierOptions: Array<{
  value: string;
  label: string;
  isDisabled: boolean;
}> = fieldDisplayKeys.map(fieldIdentifierType => {
  const divider = '──────────';
  const { value = fieldIdentifierType, displayAs: label = divider } = fieldIdentifierTypes[fieldIdentifierType] || {};

  // Adds a divider for a value of _
  // Visually this separates ID from none fieldIdentifierTypes
  return {
    value,
    label,
    isDisabled: fieldIdentifierType === '_'
  };
});

export { compliancePolicyStrings, fieldIdentifierOptions };
