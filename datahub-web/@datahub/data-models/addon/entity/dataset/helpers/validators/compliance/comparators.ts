import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';

/**
 * Keys that have been arbitrarily determined to be relevant in regards to determining whether two tags are saying
 * the same thing annotation wise. Excluded keys would generally mean that the key is either a readonly key that is
 * decorated by the API or a key that is generally more about rendering helpers than identifying the compliance
 * qualities of the tag
 * @type {Array<keyof DatasetComplianceAnnotation>}
 */
const relevantKeys: Array<keyof DatasetComplianceAnnotation> = [
  'identifierField',
  'identifierType',
  'logicalType',
  'isPurgeKey',
  'valuePattern'
];

/**
 * Returns whether two values are the same (by strict match) or are both falsy values (such as null or undefined)
 * @param valueA - first value for comparison
 * @param valueB - second value for comparison
 */
const valuesMatchOrAreFalsy = (valueA: unknown, valueB: unknown): boolean => valueA === valueB || (!valueA && !valueB);

/**
 * Takes two annotation tags of class DatasetComplianceAnnotation and, given a list of relevant keys for comparison, returns the
 * number of keys that are the same for both sides. This will help when we want to calculate how similar two tags are.
 * @param basline - annotation to compare to
 * @param annotation - annotation to be compared
 */
const countMatchingRelevantKeysBetweenAnnotations = (
  basline: DatasetComplianceAnnotation,
  annotation: DatasetComplianceAnnotation
): number =>
  relevantKeys.reduce(
    // We compare whether key values match, or if they are both falsy values because if both values are null or undefined, or an
    // empty string they are for all intents and purposes the same
    (matches, key) => matches + (valuesMatchOrAreFalsy(basline[key], annotation[key]) ? 1 : 0),
    0
  );

/**
 * Given a list of relevant keys, determine if two tags are functionally the same by matching on all relevant keys
 * @param baseline - annotation to compare to
 * @param annotation - annotation to be compared
 */
export const annotationsMatchByRelevantKeys = (
  baseline: DatasetComplianceAnnotation,
  annotation: DatasetComplianceAnnotation
): boolean => countMatchingRelevantKeysBetweenAnnotations(baseline, annotation) === relevantKeys.length;
