/**
 * Tier of a machine learning feature. See go/datahub/proml/ml-feature-tiers for more info
 * @export
 * @enum {string}
 */
export enum FeatureTierType {
  // The feature is deprecated and is not recommended for external adoption
  Deprecated = 'DEPRECATED',
  // The feature producer would like to keep the feature used by the people listed as owners. The feature might or might not be production ready
  // Commenting PRIVATE Tier out so that it is hidden from the user. We want to make everything PUBLIC Tier since all the features are already in frame
  // Uncomment below if PRIVATE option for Tier needs to be re-enabled for user selection
  //Private = 'PRIVATE',
  // The feature is published for use by other teams. However there is no commitment for feature maintenance and monitoring by the feature owner.
  // The feature user should contact the feature owner for maintenance contract",
  Public = 'PUBLIC',
  // This feature is supposed to be a Horizontal Feature. The feature is published for use by other teams.
  // And the feature producer is committed to maintain and monitor the feature in the whole feature life cycle before deprecation.
  PublicProduction = 'PUBLIC_PRODUCTION'
}
