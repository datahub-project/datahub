/**
 * Describes the interface for an avatar object
 * @interface IAvatar
 */
interface IAvatar {
  imageUrl: string;
  email?: null | string;
  // Handle for the avatar
  userName?: string;
  name?: string;
}

export { IAvatar };
