/**
 * The expected types of social actions that a user can take on an entity.
 */
export enum SocialAction {
  // Gives an upvote to the entity to endorse its usefulness
  LIKE = 'like',
  // Opts into notifications for the entity's changes in metadata
  FOLLOW = 'follow',
  // Saves the entity to a specified list, organized by the user for some organizational benefit
  SAVE = 'save'
}
