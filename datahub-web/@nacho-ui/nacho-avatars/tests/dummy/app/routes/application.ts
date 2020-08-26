import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import NachoAvatarService from '@nacho-ui/avatars/services/nacho-avatars';

export default class Application extends Route {
  @service('nacho-avatars')
  avatarsService!: NachoAvatarService;

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);
    // Sample of a time to set the avatar service fallback url
    this.avatarsService.set('imgFallbackUrl', 'http://cdn.akc.org/content/hero/puppy-boundaries_header.jpg');
  }
}
