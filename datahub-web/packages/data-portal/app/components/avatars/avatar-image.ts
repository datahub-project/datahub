import Component from '@ember/component';
import { IAvatar } from 'datahub-web/typings/app/avatars';
import { alias } from '@ember/object/computed';
import { set } from '@ember/object';
import { classNames, tagName, attribute } from '@ember-decorators/component';
import { task } from 'ember-concurrency';
import { run } from '@ember/runloop';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

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
  src: IAvatar['imageUrl'];

  /**
   * Aliases the name property on the related avatar
   * @type {ComputedProperty<IAvatar['name']>}
   */
  @attribute('alt')
  @alias('avatar.name')
  name: IAvatar['name'];

  /**
   * Handler for image error event
   * @memberof AvatarImage
   */
  @attribute
  onerror = (): void => {
    // Change avatar if an error occurs when the image is loaded

    run(() => this.onImageFallback.perform());
  };

  /**
   * Task to set the fallback image for an avatar
   * @memberof AvatarImage
   */
  @task(function*(this: AvatarImage): IterableIterator<void> {
    set(this, 'src', this.avatar.imageUrlFallback || '/assets/images/default_avatar.png');
  })
  onImageFallback!: ETaskPromise<void>;
}
