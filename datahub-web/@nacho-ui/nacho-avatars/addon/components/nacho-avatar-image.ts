import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-avatar-image';
import { inject as service } from '@ember/service';
import { classNames, tagName, attribute } from '@ember-decorators/component';
import { alias } from '@ember/object/computed';
import NachoAvatarService from '../services/nacho-avatars';
import { set } from '@ember/object';

/**
 * The nacho avatar image is used when displaying a rounded profile picture for a particular
 * individual. Comes with an added benefit of falling back to a specified url image
 *
 * @example
 * {{nacho-avatar-image
 *   img="string.url"
 *   altText="optionalString"
 * }}
 */
@tagName('img')
@classNames('nacho-avatar')
export default class NachoAvatarImage extends Component {
  layout = layout;

  /**
   * Passed in string for the expected src url. Gets computed into the image source
   * @type {string}
   */
  img!: string;

  /**
   * Passed in string for the alt text. Gets computed into the alt attribute
   * @type {string}
   */
  altText!: string;

  /**
   * The service used to capture the fallback image url configuration for the avatars
   * @type {ComputedProperty<NachoAvatarService>}
   */
  @service('nacho-avatars')
  avatarService!: NachoAvatarService;

  /**
   * Uses the service to capture the fallback image url configuration in case our initial url
   * fails.
   * @type {ComputedProperty<NachoAvatarService['imgFallbackUrl']}
   */
  @alias('avatarService.imgFallbackUrl')
  fallbackUrl!: string;

  /**
   * Based on given url from the consumer, sets the src attribute of our image to fetch
   * @type {ComputedProperty<NachoAvatarImage['img']}
   */
  @attribute('src')
  @alias('img')
  src!: NachoAvatarImage['img'];

  /**
   * Based on given alt text from the consumer, sets the alt attribute of our image on failure of
   * fallbackurl
   * @type {ComputedProperty<NachoAvatarImage['altText']}
   */
  @attribute('alt')
  @alias('altText')
  alt!: NachoAvatarImage['altText'];

  /**
   * Fallback image gets set if the main url fails in this method
   * @type {() => void}
   */
  @attribute
  onerror = (): void => {
    !this.isDestroyed && set(this, 'src', this.fallbackUrl);
  };
}
