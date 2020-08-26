/**
 * Status of the feature. Used to define the status of the feature (whether it is published)
 * @export
 * @enum {string}
 */
export enum FeatureStatusType {
  // Feature owner approved the feature. It should be viewed and used by all feature users
  Published = 'PUBLISHED',
  // Feature owner is editing the feature or not yet published the feature
  Unpublished = 'UNPUBLISHED',
  // Feature owner already deleted feature in frame MP
  Deleted = 'DELETED',
  // Feature as not suitable to show, either deprecated, being marked as not healthy or other reasons determined by owner
  Hidden = 'HIDDEN',
  // Feature information is not completed by owner
  Incomplete = 'INCOMPLETE'
}
