import Component from '@ember/component';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { set, get, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { action } from '@ember-decorators/object';
import { singularize, pluralize } from 'ember-inflector';

export default class RollupAvatars extends Component {
  tagName = 'span';

  classNames = ['avatar-rollup'];

  constructor() {
    super(...arguments);

    this.avatars || (this.avatars = []);
    this.avatarType || (this.avatarType = 'entity');
  }

  /**
   * References the full list of avatars
   * @type {Array<IAvatar>}
   */
  avatars: Array<IAvatar>;

  /**
   * Flag indicating if the avatars detail view should be rendered
   * @type {boolean}
   */
  isShowingAvatars = false;

  /**
   * The type of avatars being shown
   * @type {string}
   */
  avatarType: string;

  /**
   * Returns the text to be shown in the avatar detail page header
   * @type {ComputedProperty<string>}
   */
  header: ComputedProperty<string> = computed('avatars.length', function(): string {
    const count = get(this, 'avatars').length;
    const suffix = get(this, 'avatarType');

    return `${count} ${count > 1 ? pluralize(suffix) : singularize(suffix)}`;
  });

  /**
   * Handles the component click event
   */
  click() {
    set(this, 'isShowingAvatars', true);
  }

  /**
   * Updates the flag indicating if the view should be rendered
   */
  @action
  dismissAvatars() {
    set(this, 'isShowingAvatars', false);
  }
}
