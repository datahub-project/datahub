import Service from '@ember/service';
import { assert } from '@ember/debug';
import { appConfigUrl } from 'wherehows-web/utils/api/configurator/configurator';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { IAppConfig, IConfiguratorGetResponse } from 'wherehows-web/typings/api/configurator/configurator';
import { ApiStatus } from 'wherehows-web/utils/api';
import deepClone from 'wherehows-web/utils/deep-clone';

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
  async load(): Promise<IAppConfig> {
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
   * Returns the last saved configuration object if one was successfully retrieved
   * @param {String} key if provided, the value is returned with that key on the config hash is returned
   * @return {any}
   */
  getConfig<T>(key?: string): IAppConfig | T {
    assert('Please ensure you have invoked the `load` method successfully prior to calling `getConfig`.', configLoaded);

    // Ensure that the application configuration has been successfully cached
    if (key) {
      return deepClone<T>(appConfig[key]);
    }

    return deepClone(appConfig);
  }
}
