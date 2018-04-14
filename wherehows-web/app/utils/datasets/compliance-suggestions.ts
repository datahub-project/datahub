import { lowQualitySuggestionConfidenceThreshold } from 'wherehows-web/constants';
import { arrayMap } from 'wherehows-web/utils/array';
import { IComplianceChangeSet, ISuggestedFieldTypeValues } from 'wherehows-web/typings/app/dataset-compliance';

/**
 * Takes a list of suggestions with confidence values, and if the confidence is greater than
 * a low confidence threshold
 * @param {number} confidenceLevel percentage indicating how confidence the system is in the suggested value
 * @return {boolean}
 */
const isHighConfidenceSuggestion = ({ confidenceLevel = 0 }: { confidenceLevel: number }): boolean =>
  confidenceLevel > lowQualitySuggestionConfidenceThreshold;

/**
 * Extracts the tag suggestion from an IComplianceChangeSet tag.
 * If a suggestionAuthority property exists on the tag, then the user has already either accepted or ignored
 * the suggestion for this tag. It's value should not be taken into account on re-renders,
 * in place, this substitutes an empty suggestion
 * @param {IComplianceChangeSet} tag
 * @return {{identifierType: IComplianceChangeSet.identifierType, logicalType: IComplianceChangeSet.logicalType, confidence: number} | void}
 */
const getTagSuggestions = (tag: IComplianceChangeSet = <IComplianceChangeSet>{}): ISuggestedFieldTypeValues | void => {
  const { suggestion } = tag;

  if (suggestion && isHighConfidenceSuggestion(suggestion)) {
    const { identifierType, logicalType, confidenceLevel: confidence } = suggestion;
    return { identifierType, logicalType, confidence: +(confidence * 100).toFixed(2) };
  }
};

/**
 * Gets the suggestions for a list of IComplianceChangeSet fields
 * @type {(array: Array<IComplianceChangeSet>) => Array<{identifierType: ComplianceFieldIdValue | NonIdLogicalType | null; logicalType: IdLogicalType | null; confidence: number} | void>}
 */
const getTagsSuggestions = arrayMap(getTagSuggestions);

export { isHighConfidenceSuggestion, getTagSuggestions, getTagsSuggestions };
