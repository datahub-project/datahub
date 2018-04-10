import Component from '@ember/component';
import ComputedProperty, { alias } from '@ember/object/computed';
import { IAvatar } from 'wherehows-web/typings/app/avatars';

export default class AvatarImage extends Component {
  tagName = 'img';

  classNames = ['avatar'];

  attributeBindings = ['src', 'name:alt'];

  /**
   * The avatar object to render
   * @type {IAvatar}
   */
  avatar: IAvatar;

  /**
   * Returns the image url for the avatar
   * @type {ComputedProperty<IAvatar['imageUrl']>}
   */
  src: ComputedProperty<IAvatar['imageUrl']> = alias('avatar.imageUrl');

  /**
   * Aliases the name property on the related avatar
   * @type {ComputedProperty<IAvatar['name']>}
   */
  name: ComputedProperty<IAvatar['name']> = alias('avatar.name');
}
