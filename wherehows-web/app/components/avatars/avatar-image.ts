import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { alias } from '@ember-decorators/object/computed';
import { set } from '@ember/object';
import { classNames, tagName, attribute } from '@ember-decorators/component';

@tagName('img')
@classNames('avatar')
export default class AvatarImage extends Component {
  /**
   * The avatar object to render
   * @type {IAvatar}
   */
  avatar!: IAvatar;

  /**
   * Returns the image url for the avatar
   * @type {ComputedProperty<IAvatar['imageUrl']>}
   */
  @attribute
  @alias('avatar.imageUrl')
  src: ComputedProperty<IAvatar['imageUrl']>;

  /**
   * Aliases the name property on the related avatar
   * @type {ComputedProperty<IAvatar['name']>}
   */
  @attribute('alt')
  @alias('avatar.name')
  name: ComputedProperty<IAvatar['name']>;

  /**
   * Handler for image error event
   * @memberof AvatarImage
   */
  @attribute
  onerror = (): void => {
    // Change avatar if an error occurs when the image is loaded
    this.onImageFallback();
  };

  /**
   * Sets the fallback image for an avatar
   * @memberof AvatarImage
   */
  onImageFallback(): void {
    set(this, 'src', this.avatar.imageUrlFallback);
  }
}
