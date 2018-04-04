import Component from '@ember/component';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { action } from 'ember-decorators/object';

export default class extends Component {
  tagName: 'span';

  classNames = ['avatar-metadata'];

  /**
   * Reference to avatar containing metadata
   * @type {IAvatar}
   */
  avatar: IAvatar;

  /**
   * Sends email to avatar
   */
  @action
  emailAvatar() {}

  /**
   * Slacks avatar
   */
  @action
  slackAvatar() {}
}
