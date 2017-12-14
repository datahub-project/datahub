import Ember from 'ember';
import { ComplianceFieldIdValue, IdLogicalType } from 'wherehows-web/constants/datasets/compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { arrayMap } from 'wherehows-web/utils/array';

const { String: { htmlSafe } } = Ember;

/**
 * Defines the interface field identifier drop downs
 * @interface IFieldIdentifierOption
 */
interface IFieldIdentifierOption {
  value: string;
  label: string;
  isDisabled?: boolean;
}

/**
 * Defines the interface for compliance data type field options
 * @interface IComplianceFieldIdentifierOption
 * @extends {IFieldIdentifierOption}
 */
interface IComplianceFieldIdentifierOption extends IFieldIdentifierOption {
  value: ComplianceFieldIdValue;
}

interface IComplianceFieldFormatOption {
  value: IdLogicalType | null;
  label: string;
  isDisabled?: boolean;
}

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
 * Takes a compliance data type and transforms it into a compliance field identifier option
 * @param {IComplianceDataType} complianceDataType 
 * @returns {IComplianceFieldIdentifierOption}
 */
const getFieldIdentifierOption = (complianceDataType: IComplianceDataType): IComplianceFieldIdentifierOption => {
  const { id, title } = complianceDataType;
  return { value: id, label: title };
};

/**
 * Maps over a list of compliance data types objects and transforms to a list of dropdown options
 * @type {(array: Array<IComplianceDataType>) => Array<IComplianceFieldIdentifierOption>}
 */
const getFieldIdentifierOptions = arrayMap(getFieldIdentifierOption);

/**
 * Defines the html string for informing the user of hidden tracking fields
 * @type {Ember.String.htmlSafe}
 */
const hiddenTrackingFields = htmlSafe(
  '<p>Some fields in this dataset have been hidden from the table(s) below. ' +
    "These are tracking fields for which we've been able to predetermine the compliance classification.</p>" +
    '<p>For example: <code>header.memberId</code>, <code>requestHeader</code>. ' +
    'Hopefully, this saves you some scrolling!</p>'
);

/**
 * Defines the sequence of edit steps in the compliance policy component
 */
const complianceSteps = {
  0: {
    name: 'editCompliancePolicy'
  },
  1: {
    name: 'editPurgePolicy'
  },
  2: {
    name: 'editDatasetClassification'
  }
};

/**
 * Takes a map of dataset options and constructs the relevant compliance edit wizard steps to build the wizard flow
 * @param {boolean} [hasSchema=true] flag indicating if the dataset has a schema or otherwise
 * @returns {({ [x: number]: { name: string } })}
 */
const getComplianceSteps = (hasSchema: boolean = true): { [x: number]: { name: string } } => {
  // Step to tag dataset with PII data, this is at the dataset level for schema-less datasets
  const piiTaggingStep = { 0: { name: 'editDatasetLevelCompliancePolicy' } };

  if (!hasSchema) {
    return { ...complianceSteps, ...piiTaggingStep };
  }

  return complianceSteps;
};

export {
  compliancePolicyStrings,
  getFieldIdentifierOption,
  getFieldIdentifierOptions,
  complianceSteps,
  hiddenTrackingFields,
  getComplianceSteps,
  IFieldIdentifierOption,
  IComplianceFieldIdentifierOption,
  IComplianceFieldFormatOption
};
