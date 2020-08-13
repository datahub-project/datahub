/**
 * Represents a Like aspect on an entity, each ILikeAction represents an upvote from a specific
 * user for a specific entity to which the aspect would be attached
 */
export interface ILikeAction {
  // urn of the user who has liked a specified entity
  likedBy: string;
  // information on the audit stamp of the like action
  lastModified?: Com.Linkedin.Common.AuditStamp;
}

/**
 * The actual aspect object that would be attached to an entity.
 */
export interface ILikesAspect {
  // The list of like actions connected to the list of users who have liked a particular entity
  actions: Array<ILikeAction>;
}

// TODO: [META-11091] Retrieve these from Com.Linkedin.Common instead, once PDSC to TS is updated

/**
 * An identifier for the entity that has decided to follow a particular entity
 */
export interface IFollowerType {
  // A single user entity
  corpUser?: string;
  // A group entity, consisting of multiple users
  corpGroup?: string;
}

/**
 * A follow action that represents the subscription of a user or group to this entity for
 * notifications
 */
export interface IFollowAction {
  // Object identifying the information for the follower entity
  follower: IFollowerType;
}

/**
 * The actual aspect object for follows attached to an entity
 */
export interface IFollowsAspect {
  // A list of follow actions related to the entity, each follow action is associated with a single
  // user or group that has subscribed to notfications
  followers: Array<IFollowAction>;
}
