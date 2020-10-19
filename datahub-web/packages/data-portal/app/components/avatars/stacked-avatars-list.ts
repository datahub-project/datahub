import Component from '@ember/component';
import { IAvatar } from 'datahub-web/typings/app/avatars';
import { action, computed } from '@ember/object';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

import { classNames } from '@ember-decorators/component';
import { noop } from 'lodash';

/**
 * Specifies the default maximum number of images to render before the more button
 * @type {number}
 */
const defaultMavAvatarLength = 6;

@classNames('avatar-container')
export default class StackedAvatarsList extends Component {
  /**
   * The list of avatar objects to render
   * @type {Array<IAvatar>}
   */
  avatars: Array<IAvatar> = [];

  /**
   * External action to selection of an avatar's menu option
   */
  handleAvatarOptionSelection: (avatar: IAvatar, option?: INachoDropdownOption<unknown>) => unknown = noop;

  /**
   * Calculates the max number of avatars to render
   * @type {ComputedProperty<number>}
   * @memberof StackedAvatarsList
   */
  @computed('avatars.length')
  get maxAvatarLength(): number {
    const {
      avatars: { length }
    } = this;
    return length ? Math.min(length, defaultMavAvatarLength) : defaultMavAvatarLength;
  }

  /**
   * Build the list of avatars to render based on the max number
   * @type {ComputedProperty<StackedAvatarsList['avatars']>}
   * @memberof StackedAvatarsList
   */
  @computed('maxAvatarLength')
  get maxAvatars(): StackedAvatarsList['avatars'] {
    const { avatars, maxAvatarLength } = this;

    return avatars.slice(0, maxAvatarLength);
  }

  /**
   * Determines the list of avatars that have not been rendered after the max has been ascertained
   * @type {ComputedProperty<StackedAvatarsList['avatars']>}
   * @memberof StackedAvatarsList
   */
  @computed('maxAvatars')
  get rollupAvatars(): StackedAvatarsList['avatars'] {
    const { avatars, maxAvatarLength } = this;

    return avatars.slice(maxAvatarLength);
  }

  /**
   * Handler to invoke IAvatarDropDownAction instance when the drop down option is selected
   * @param {IAvatar} avatar the avatar item selected from the list
   * @param {(INachoDropdownOption<unknown>)} [selectedOption] drop down option selected
   * @memberof StackedAvatarsList
   */
  @action
  onAvatarOptionSelected(avatar: IAvatar, selectedOption?: INachoDropdownOption<unknown>): void {
    this.handleAvatarOptionSelection(avatar, selectedOption);
  }
}
