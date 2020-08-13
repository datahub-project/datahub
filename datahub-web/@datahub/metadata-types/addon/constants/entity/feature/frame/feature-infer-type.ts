/**
 * Infer type of the feature. Indicates whether the feature is derived directly from fact data or inferred
 * @export
 * @enum {string}
 */
export enum FeatureInferType {
  // Feature is fact type
  Fact = 'FACT',
  // Feature is inferred type
  Inferred = 'INFERRED'
}
