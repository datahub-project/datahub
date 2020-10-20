import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

/**
 * TODO: META-9287 Move to Nacho lib
 * Describes the interface for an avatar object
 * @interface IAvatar
 */
export interface IAvatar {
  // URL for an avatar entity
  imageUrl: string;
  // Fallback url for avatar image on error
  imageUrlFallback: string;
  // Optional email for the user associated with the avatar
  email?: null | string;
  // Handle for the avatar
  userName?: string;
  // Optional name for the user
  name?: string;
  // If the avatar leads to a profile page for the person
  profileLink?: string;
  // Selection options for an avatar with dropdown
  avatarOptions?: Array<INachoDropdownOption<unknown>>;
}

/**
 *  Properties for an avatar entity
 * @interface IUserEntityProps
 */
export interface IUserEntityProps {
  // Primary URL for the avatar image source
  aviUrlPrimary: string;
  // Secondary URL for the avatar image source, used as a fallback if the primary is unavailable
  aviUrlFallback: string;
}
