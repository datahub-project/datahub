/**
 * Defines a map of values for the compliance policy on a dataset
 * @type {Object}
 */
const compliancePolicyStrings = {
  complianceDataException: 'Unexpected discrepancy in compliance data',
  missingTypes: 'Looks like some fields may contain privacy data but do not have a specified `Field Format`?',
  successUpdating: 'Your changes have been successfully saved!',
  failedUpdating: 'Oops! We are having trouble updating this dataset at the moment.',
  helpText: {
    classification:
      'The default value is taken from go/dht and should be good enough in most cases. ' +
        'You can optionally override it if required by house security.'
  }
};

export { compliancePolicyStrings };
