import { lowQualitySuggestionConfidenceThreshold } from 'wherehows-web/constants';

/**
 * Takes a list of suggestions with confidence values, and if the confidence is greater than
 * a low confidence threshold
 * @param {number} confidenceLevel percentage indicating how confidence the system is in the suggested value
 * @return {boolean}
 */
const isHighConfidenceSuggestion = ({ confidenceLevel = 0 }: { confidenceLevel: number }): boolean =>
  confidenceLevel > lowQualitySuggestionConfidenceThreshold;

export { isHighConfidenceSuggestion };
