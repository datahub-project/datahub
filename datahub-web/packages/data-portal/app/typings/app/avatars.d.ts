import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

/**
 * Describes the interface for an avatar object
 * @interface IAvatar
 */
export interface IAvatar {
  // URL for an avatar entity
  imageUrl: string;
  // Fallback url for avatar image on error
  imageUrlFallback: string;
  email?: null | string;
  // Handle for the avatar
  userName?: string;
  name?: string;
  // If the avatar leads to a profile page for the person
  profileLink?: string;
  // Selection options for an avatar with dropdown
  avatarOptions?: Array<INachoDropdownOption<unknown>>;
}
