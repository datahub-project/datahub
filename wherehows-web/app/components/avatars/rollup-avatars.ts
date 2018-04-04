import Component from '@ember/component';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { set, get, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { action } from 'ember-decorators/object';

export default class extends Component {
  tagName = 'span';

  classNames = ['avatar-rollup'];

  constructor() {
    super(...arguments);

    this.avatars || (this.avatars = []);
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
   * Returns the text to be shown in the avatar detail page header
   * @type {ComputedProperty<string>}
   */
  header: ComputedProperty<string> = computed('avatars.length', function(): string {
    return `${get(this, 'avatars').length} users`;
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
