import { IComplianceChangeSet } from 'wherehows-web/components/dataset-compliance';
import { lowQualitySuggestionConfidenceThreshold } from 'wherehows-web/constants';
import { arrayMap } from 'wherehows-web/utils/array';

/**
 * Takes a list of suggestions with confidence values, and if the confidence is greater than
 * a low confidence threshold
 * @param {number} confidenceLevel percentage indicating how confidence the system is in the suggested value
 * @return {boolean}
 */
const isHighConfidenceSuggestion = ({ confidenceLevel = 0 }: { confidenceLevel: number }): boolean =>
  confidenceLevel > lowQualitySuggestionConfidenceThreshold;

/**
 * Extracts the field suggestion from an IComplianceChangeSet field.
 * If a suggestionAuthority property exists on the field, then the user has already either accepted or ignored
 * the suggestion for this field. It's value should not be taken into account on re-renders,
 * in place, this substitutes an empty suggestion
 * @param {IComplianceChangeSet} field
 * @return {{identifierType: IComplianceChangeSet.identifierType, logicalType: IComplianceChangeSet.logicalType, confidence: number} | void}
 */
const getFieldSuggestions = (
  field: IComplianceChangeSet = <IComplianceChangeSet>{}
): {
  identifierType: IComplianceChangeSet['identifierType'];
  logicalType: IComplianceChangeSet['logicalType'];
  confidence: number;
} | void => {
  const { suggestion } = field.hasOwnProperty('suggestionAuthority') ? { suggestion: void 0 } : field;

  if (suggestion && isHighConfidenceSuggestion(suggestion)) {
    const { identifierType, logicalType, confidenceLevel: confidence } = suggestion;
    return { identifierType, logicalType, confidence: +(confidence * 100).toFixed(2) };
  }
};

/**
 * Gets the suggestions for a list of IComplianceChangeSet fields
 * @type {(array: Array<IComplianceChangeSet>) => Array<{identifierType: ComplianceFieldIdValue | NonIdLogicalType | null; logicalType: IdLogicalType | null; confidence: number} | void>}
 */
const getFieldsSuggestions = arrayMap(getFieldSuggestions);

export { isHighConfidenceSuggestion, getFieldSuggestions, getFieldsSuggestions };
