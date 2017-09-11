import Ember from 'ember';
import { avatar } from 'wherehows-web/constants';

const { Component, computed, get } = Ember;
const { fallbackUrl, url }: { fallbackUrl: string; url: string } = avatar;
const headlessUserName: string = 'wherehows';

export default Component.extend({
  tagName: 'span',

  /**
   * @type {string} username for the user, e.g. ldap userName that can be used to construct the url
   */
  userName: '',

  /**
   * Ember.ComputerProperty that resolves with the image url for the avatar
   * @type {string}
   */
  imageUrl: computed('userName', function(this: Ember.Component) {
    const userName = get(this, 'userName');
    if (userName && userName !== headlessUserName) {
      return url.replace('[username]', userName);
    }

    return fallbackUrl;
  })
});
