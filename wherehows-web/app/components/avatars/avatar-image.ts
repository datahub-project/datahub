import Component from '@ember/component';
import ComputedProperty, { alias } from '@ember/object/computed';
import { computed, get } from '@ember/object';
import { IAvatar } from 'wherehows-web/typings/app/avatars';

export default class AvatarImage extends Component {
  tagName = 'img';

  classNames = ['avatar'];

  attributeBindings = ['src', 'alt:name'];

  /**
   * The avatar object to render
   * @type {IAvatar}
   */
  avatar: IAvatar;

  /**
   * Returns the image url for the avatar
   * @type {ComputedProperty<string>}
   */
  src: ComputedProperty<string | void> = computed('avatar.imageUrl', function(this: AvatarImage): string | void {
    //@ts-ignore dot notation property access
    return get(this, 'avatar.imageUrl');
  });

  /**
   * Aliases the name property on the related avatar
   * @type {ComputedProperty<IAvatar['name']>}
   */
  name: ComputedProperty<IAvatar['name']> = alias('avatar.name');
}
