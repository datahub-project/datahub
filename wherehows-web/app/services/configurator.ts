import Service from '@ember/service';
import { assert } from '@ember/debug';
import { appConfigUrl } from 'wherehows-web/utils/api/configurator/configurator';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { IAppConfig, IConfiguratorGetResponse } from 'wherehows-web/typings/api/configurator/configurator';
import { ApiStatus } from 'wherehows-web/utils/api';
import deepClone from 'wherehows-web/utils/deep-clone';

/**
 * Conditional type alias for getConfig return type, if T is assignable to a key of
 * IAppConfig, then return the property value, otherwise returns the  IAppConfig object
 */
type IAppConfigOrProperty<T> = T extends keyof IAppConfig ? IAppConfig[T] : T extends undefined ? IAppConfig : never;

/**
 * Holds the application configuration object
 * @type {IAppConfig}
 */
const appConfig: IAppConfig = <IAppConfig>{};

/**
 * Flag indicating the config object has been successfully loaded from the remote endpoint
 * @type {boolean}
 */
let configLoaded = false;

export default class Configurator extends Service {
  /**
   * Fetches the application configuration object from the provided endpoint and augments the appConfig object
   * @return {Promise<IAppConfig>}
   */
  static async load(): Promise<IAppConfig> {
    try {
      const { status, config } = await getJSON<IConfiguratorGetResponse>({ url: appConfigUrl });

      if (status === ApiStatus.OK) {
        return (configLoaded = true) && Object.assign(appConfig, config);
      }

      return Promise.reject(new Error(`Configuration load failed with status: ${status}`));
    } catch (e) {
      configLoaded = false;

      return Promise.reject(e);
    }
  }

  /**
   * Returns a copy of the last saved configuration object if one was successfully retrieved,
   * or a copy of the property on the IAppConfig object, if specified
   * @static
   * @template K
   * @param {K} [key] if provided, the value is returned with that key on the config hash is returned
   * @param {IAppConfigOrProperty<K>} [defaultValue] if provided, will default if key is not found in config
   * @returns {IAppConfigOrProperty<K>}
   * @memberof Configurator
   */
  static getConfig<K extends keyof IAppConfig | undefined>(
    key?: K,
    defaultValue?: IAppConfigOrProperty<K>
  ): IAppConfigOrProperty<K> {
    // Ensure that the application configuration has been successfully cached
    assert('Please ensure you have invoked the `load` method successfully prior to calling `getConfig`.', configLoaded);

    return typeof key === 'string' && appConfig.hasOwnProperty(<keyof IAppConfig>key)
      ? <IAppConfigOrProperty<K>>deepClone(appConfig[<keyof IAppConfig>key])
      : defaultValue !== undefined
        ? defaultValue
        : <IAppConfigOrProperty<K>>deepClone(appConfig);
  }
}
