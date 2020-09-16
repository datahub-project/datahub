import Service from '@ember/service';
import { IUserSettings } from '@datahub/shared/types/user-settings/user-settings';

/**
 * Service that collects and exposes the user given settings.
 * Note: Currently, there is no backend to support user settings, so we are relying on local storage since most users
 * will use the same device (their work computer) to log into DataHub. Since we are exposing through the service though
 * other components will not need to concern themselves with that and the change from local storage to a proper API
 * can just be done here
 */
export default class UserSettingsService extends Service {
  /**
   * The key to which we will store our settings in local storage
   */
  private static userSettingsLocalStorageKey = 'datahub-user-settings';

  /**
   * The fetched or defaulted settings, used as a kv store when a component tries to fetch or set
   */
  settings: IUserSettings;

  /**
   * Sets a new local storage with the default user settings if we were for some reason unable to read them
   */
  private setNewLocalStorage(userSettings: IUserSettings): void {
    localStorage.setItem(UserSettingsService.userSettingsLocalStorageKey, JSON.stringify(userSettings));
  }

  /**
   * Gets a default set of user settings if we can't read them
   */
  private getDefaultUserSettings(): IUserSettings {
    return {};
  }

  /**
   * Saves the current state of the user settings to persist them
   */
  private saveAndPersistUserSettings(): void {
    localStorage.setItem(UserSettingsService.userSettingsLocalStorageKey, JSON.stringify(this.settings));
  }

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);

    const { getDefaultUserSettings, setNewLocalStorage } = this;
    let userSettings: IUserSettings | null = null;

    try {
      const localStorage = window.localStorage;
      const storedUserSettings = localStorage.getItem(UserSettingsService.userSettingsLocalStorageKey);
      if (storedUserSettings) {
        userSettings = JSON.parse(storedUserSettings);
      } else {
        userSettings = getDefaultUserSettings();
        setNewLocalStorage(userSettings);
      }
    } catch (e) {
      // If the error above happened because we couldn't parse any settings, then we still want to assign a default
      // here
      if (!userSettings) {
        userSettings = getDefaultUserSettings();
        setNewLocalStorage(userSettings);
      }
    }

    this.settings = userSettings as IUserSettings;
  }

  /**
   * Given a key expected in our user settings, retrieves the value currently saved for that setting
   * @param key key to define which user setting we are looking for
   * @param options (optional) additional options we can specify such as default value
   */
  getUserSetting<K extends keyof IUserSettings>(key: K, options?: { default?: IUserSettings[K] }): IUserSettings[K] {
    const fetchedValue = this.settings[key];
    const hasDefaultValue = options?.default !== undefined;
    const defaultValue = options?.default as IUserSettings[K];
    return fetchedValue === undefined && hasDefaultValue ? defaultValue : this.settings[key];
  }

  /**
   * Given a key expected in our user settings and a compatible value, saves and persists that setting
   * @param key key to define which user setting we want to save
   * @param value value to save to that setting
   */
  setUserSetting<K extends keyof IUserSettings, T extends IUserSettings[K]>(key: K, value: T): void {
    this.settings[key] = value;
    this.saveAndPersistUserSettings();
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // This is a core ember thing
  //eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'user-settings': UserSettingsService;
  }
}
