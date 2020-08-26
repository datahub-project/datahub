import Component from '@glimmer/component';
import { Avatar } from '@datahub/shared/modules/avatars/avatar';

/**
 * The types available for name display
 */
type DisplayType = 'name' | 'username';

interface IAvatarNameComponenetArgs {
  // A single instance of the Avatar class, representing the person whose name we want to show in
  // this component
  avatar: Avatar;
  // Base class to use in addition to the base avatar name component class, if needed
  nameClass?: string;
  // Option passed in to select type of name displayed
  displayType?: DisplayType;
}

export const baseAvatarNameComponentClass = 'avatar-name';

/**
 * The AvatarNameComponent is responsible for rendering an avatar when there is a single name
 * to be shown for the person. It should be used in conjunction with the main avatar container
 * @example
 * <Avatar::Containers::AvatarMain ...props as |avatars|>
 *   {{#each avatars as |avatar|}}
 *     <Avatar::AvatarName @avatar={{avatar}} />
 *   {{/each}}
 * </Avatar::Containers::AvatarMain>
 */
export default class AvatarNameComponent extends Component<IAvatarNameComponenetArgs> {
  /**
   * Attaching to component for ease of access in the template
   */
  baseClass = baseAvatarNameComponentClass;

  /**
   * The intended name type to be displayed
   */
  get displayType(): DisplayType {
    return this.args.displayType || 'username';
  }
}
