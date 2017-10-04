import { IFieldSuggestion, IPrediction } from 'wherehows-web/typings/api/datasets/compliance';
import { arrayFilter, arrayReduce } from 'wherehows-web/utils/array';
import { fieldIdentifierTypeValues, lowQualitySuggestionConfidenceThreshold } from 'wherehows-web/constants';

/**
 * Takes a list of suggestions with confidence values, and if the confidence is greater than
 * a low confidence threshold
 * @param {number} confidence
 * @return {boolean}
 */
const isHighConfidenceSuggestion = ({ confidence = 0 }: IPrediction): boolean =>
  confidence > lowQualitySuggestionConfidenceThreshold;

/**
 * Filters out a list of IPrediction 's that have a confidence level higher than the low confidence threshold
 * @type {(array: Array<IPrediction>) => Array<IPrediction>}
 */
const highConfidenceSuggestions = arrayFilter(isHighConfidenceSuggestion);

/**
 * Extracts the type (identifierType&|logicalType) suggestion and confidence value from a predicted or suggested object.
 * A determination is made based of the type of the value string, rather than the keys of the wrapping object:
 * `identifierTypePrediction` or `logicalTypePrediction`.
 * This is important to pay attention to when modifying the implementation
 * @param {IFieldSuggestion} suggestion the extracted suggestion
 * @param {string} value the value in the api provided suggestion
 * @param {number} confidence how confidence the system is in the suggested value
 * @return {IFieldSuggestion}
 */
const extractTypesSuggestion = (
  suggestion: IFieldSuggestion,
  { value, confidence = 0 }: IPrediction
): IFieldSuggestion => {
  if (value) {
    if (fieldIdentifierTypeValues.includes(value)) {
      suggestion = { ...suggestion, identifierType: value };
    } else {
      suggestion = { ...suggestion, logicalType: value };
    }

    // identifierType value should be the last element in the list
    return { ...suggestion, confidence: +(confidence * 100).toFixed(2) };
  }
  return suggestion;
};

const accumulateFieldSuggestions = arrayReduce(extractTypesSuggestion, <IFieldSuggestion>{ confidence: 0 });

export { highConfidenceSuggestions, accumulateFieldSuggestions };
