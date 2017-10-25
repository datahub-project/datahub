import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { avatar } from 'wherehows-web/constants';
import { computed, get } from '@ember/object';

const { fallbackUrl, url }: { fallbackUrl: string; url: string } = avatar;
const headlessUserName: string = 'wherehows';

export default class UserAvatar extends Component {
  tagName = 'span';

  /**
   * username for the user, e.g. ldap userName that can be used to construct the url
   * @type {string} 
   */
  userName: string;

  /**
   * Ember.ComputedProperty that resolves with the image url for the avatar
   * @type {ComputedProperty<string>}
   * @memberof UserAvatar
   */
  imageUrl: ComputedProperty<string> = computed('userName', function(this: UserAvatar) {
    const userName = get(this, 'userName');
    if (userName && userName !== headlessUserName) {
      return url.replace('[username]', userName);
    }

    return fallbackUrl;
  });
}
