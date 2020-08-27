import Component from '@ember/component';
import { IAvatar } from 'datahub-web/typings/app/avatars';
import { set } from '@ember/object';
import { action, computed } from '@ember/object';
import { singularize, pluralize } from 'ember-inflector';
import { tagName, classNames } from '@ember-decorators/component';

@tagName('span')
@classNames('avatar-rollup')
export default class RollupAvatars extends Component {
  /**
   * References the full list of avatars
   * @type {Array<IAvatar>}
   */
  avatars: Array<IAvatar> = [];

  /**
   * Flag indicating if the avatars detail view should be rendered
   * @type {boolean}
   */
  isShowingAvatars = false;

  /**
   * The type of avatars being shown
   * @type {string}
   */
  avatarType = 'entity';

  /**
   * Returns the text to be shown in the avatar detail page header
   * @type {string}
   */
  @computed('avatars.length')
  get header(): string {
    const count = this.avatars.length;
    const suffix = this.avatarType;

    return `${count} ${count > 1 ? pluralize(suffix) : singularize(suffix)}`;
  }

  /**
   * Handles the component click event
   */
  click(): void {
    set(this, 'isShowingAvatars', true);
  }

  /**
   * Updates the flag indicating if the view should be rendered
   */
  @action
  dismissAvatars(): void {
    set(this, 'isShowingAvatars', false);
  }
}
