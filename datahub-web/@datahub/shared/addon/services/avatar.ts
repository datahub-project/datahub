import Service from '@ember/service';

/**
 * Props that are intended to initialize the Avatar class with information external to the class
 * that need to be saved in order for the Avatar to complete its operations.
 */
export interface IAvatarConfigProps {
  // Configurable, comes from a /config API and details the root API which we would use to reach
  // the profile picture resource
  aviUrlPrimary: string;
  // Configurable, comes from a /config API and provides a fallback image resource if the primary
  // image resource cannot be found
  aviUrlFallback: string;
}

export default class AvatarService extends Service {
  /**
   * Flag to indicate if the service has run the .initWithConfigs() method yet to load required
   * config information for avatars
   */
  private hasInit = false;

  /**
   * Stores the list of configurable properties for avatars that was retrieved upon init
   */
  private configurableProperties!: IAvatarConfigProps;

  /**
   * Will throw an error if Avatar.init() has not been run yet. Insures that we are alerted if the
   * Avatar doesn't have the config information it needs to run
   */
  private throwIfNotInitialized(): void {
    if (!this.hasInit) {
      throw new Error(
        'Please ensure you have loaded the AvatarService on app startup with .initWithConfigs() before instantiating a new Avatar'
      );
    }
  }

  /**
   * Shortcut to retrieve a value for a config key for avatars
   * @param {String} key - key for the value we want to fetch
   */
  getConfig<T extends keyof IAvatarConfigProps>(key: T): IAvatarConfigProps[T] {
    this.throwIfNotInitialized();
    return this.configurableProperties[key];
  }

  /**
   * This init function should be run on app initialization, or after retrieving app-related configs
   * but before utilizing any instances of an avatar class
   * @param {IAvatarConfigProps} props - expected config properties
   */
  initWithConfigs(props: IAvatarConfigProps): void {
    this.configurableProperties = props;
    this.hasInit = true;
  }
}

declare module '@ember/service' {
  // This is a core ember thing
  //eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    avatar: AvatarService;
  }
}
