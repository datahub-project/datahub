import Service from '@ember/service';

/**
 * The nacho avatar service can be shared with the host application in order to share configs with the
 * nacho service and also ensure that these configs are shared between all nacho components that rely
 * on them in an effective and single-state way. The consuming application is also then free to set
 * these properties during any part of the application flow that they deem appropriate
 */
export default class NachoAvatarService extends Service {
  /**
   * Useful for images, when a fetch for an image fails we can use this property to fallback to some
   * default image instead of showing ugly alt text
   * @type {string}
   */
  imgFallbackUrl!: string;
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'nacho-avatars': NachoAvatarService;
  }
}
