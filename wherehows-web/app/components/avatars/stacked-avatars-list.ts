import Component from '@ember/component';
import { get, getProperties, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { IAvatar } from 'wherehows-web/typings/app/avatars';

/**
 * Specifies the default maximum number of images to render before the more button
 * @type {number}
 */
const defaultMavAvatarLength = 6;

export default class StackedAvatarsList extends Component {
  classNames = ['avatar-container'];

  constructor() {
    super(...arguments);

    this.avatars || (this.avatars = []);
  }

  /**
   * The list of avatar objects to render
   * @type {Array<IAvatar>}
   */
  avatars: Array<IAvatar>;

  /**
   * Calculates the max number of avatars to render
   * @type {ComputedProperty<number>}
   * @memberof StackedAvatarsList
   */
  maxAvatarLength: ComputedProperty<number> = computed('avatars.length', function(this: StackedAvatarsList): number {
    const { length } = get(this, 'avatars');
    return length ? Math.min(length, defaultMavAvatarLength) : defaultMavAvatarLength;
  });

  /**
   * Build the list of avatars to render based on the max number
   * @type {ComputedProperty<StackedAvatarsList['avatars']>}
   * @memberof StackedAvatarsList
   */
  maxAvatars: ComputedProperty<StackedAvatarsList['avatars']> = computed('maxAvatarLength', function(
    this: StackedAvatarsList
  ): StackedAvatarsList['avatars'] {
    const { avatars, maxAvatarLength } = getProperties(this, ['avatars', 'maxAvatarLength']);

    return avatars.slice(0, maxAvatarLength);
  });

  /**
   * Determines the list of avatars that have not been rendered after the max has been ascertained
   * @type {ComputedProperty<StackedAvatarsList['avatars']>}
   * @memberof StackedAvatarsList
   */
  rollupAvatars: ComputedProperty<StackedAvatarsList['avatars']> = computed('maxAvatars', function(
    this: StackedAvatarsList
  ): StackedAvatarsList['avatars'] {
    const { avatars, maxAvatarLength } = getProperties(this, ['avatars', 'maxAvatarLength']);
    return avatars.slice(maxAvatarLength);
  });
}
