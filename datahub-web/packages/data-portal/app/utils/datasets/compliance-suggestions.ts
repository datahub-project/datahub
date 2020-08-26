import { arrayMap } from '@datahub/utils/array/index';
import { IComplianceChangeSet, ISuggestedFieldTypeValues } from 'datahub-web/typings/app/dataset-compliance';

/**
 * Takes a list of suggestions with confidence values, and if the confidence is greater than
 * a low confidence threshold
 * @param {number} confidenceLevel percentage indicating how confidence the system is in the suggested value
 * @param {number} suggestionConfidenceThreshold threshold number to consider as a valid suggestion
 * @return {boolean}
 */
const isHighConfidenceSuggestion = (
  { confidenceLevel = 0 }: { confidenceLevel: number },
  suggestionConfidenceThreshold = 0
): boolean => confidenceLevel > suggestionConfidenceThreshold;

/**
 * Extracts the tag suggestion from an IComplianceChangeSet tag.
 * If a suggestionAuthority property exists on the tag, then the user has already either accepted or ignored
 * the suggestion for this tag. It's value should not be taken into account on re-renders,
 * in place, this substitutes an empty suggestion
 * @param {number} suggestionConfidenceThreshold confidence threshold for filtering out higher quality suggestions
 * @return {(tag?: IComplianceChangeSet) => (ISuggestedFieldTypeValues | void)}
 */
const getTagSuggestions = ({ suggestionConfidenceThreshold }: { suggestionConfidenceThreshold: number }) => (
  tag: IComplianceChangeSet = {} as IComplianceChangeSet
): ISuggestedFieldTypeValues | void => {
  const { suggestion } = tag;

  if (suggestion && isHighConfidenceSuggestion(suggestion, suggestionConfidenceThreshold)) {
    const { identifierType, logicalType, confidenceLevel: confidence } = suggestion;
    return { identifierType, logicalType, confidence: +(confidence * 100).toFixed(2) };
  }
};

/**
 * Gets the suggestions for a list of IComplianceChangeSet fields
 * @param {number} suggestionConfidenceThreshold
 * @return {(array: Array<IComplianceChangeSet>) => Array<ISuggestedFieldTypeValues | void>}
 */
const getTagsSuggestions = ({ suggestionConfidenceThreshold }: { suggestionConfidenceThreshold: number }) =>
  arrayMap(getTagSuggestions({ suggestionConfidenceThreshold }));

export { isHighConfidenceSuggestion, getTagSuggestions, getTagsSuggestions };
