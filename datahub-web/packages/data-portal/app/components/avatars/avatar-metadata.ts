import Component from '@ember/component';
import { IAvatar } from 'datahub-web/typings/app/avatars';

export default class AvatarMetadata extends Component {
  tagName: 'span';

  classNames = ['avatar-metadata'];

  /**
   * Reference to avatar containing metadata
   * @type {IAvatar}
   */
  avatar: IAvatar;

  /**
   * Slack team ID
   * @type {string}
   */
  team = 'T06BYN8F7';
}
