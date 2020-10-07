// IMPORTANT: This file is purposely `js`. Otherwise we will have unconsistent builds.
// see https://github.com/typed-ember/ember-cli-typescript/issues/724 and
// https://github.com/simplabs/ember-simple-auth/issues/1854
import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';

export default class Application extends Route {
  @service('nacho-avatars')
  avatarsService;

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);
    // Sample of a time to set the avatar service fallback url
    this.avatarsService.set('imgFallbackUrl', 'http://cdn.akc.org/content/hero/puppy-boundaries_header.jpg');
  }
}
