import Service from '@ember/service';
import { appConfigUrl } from '@datahub/shared/api/configurator/configurator';
import { getJSON } from '@datahub/utils/api/fetcher';
import { ApiStatus } from '@datahub/utils/api/shared';
import deepClone from '@datahub/utils/function/deep-clone';
import {
  IAppConfig,
  IAppConfigOrProperty,
  IConfigurator,
  IConfiguratorGetResponse
} from '@datahub/shared/types/configurator/configurator';

/**
 * Holds the application configuration object
 * @type {IAppConfig}
 */
let appConfig: Partial<IAppConfig> = {};

/**
 * Flag indicating the config object has been successfully loaded from the remote endpoint
 * @type {boolean}
 */
let configLoaded = false;

/**
 * Returns a copy of the last saved configuration object if one was successfully retrieved,
 * or a copy of the property on the IAppConfig object, if specified
 * @template K
 * @param {K} [key] if provided, the value is returned with that key on the config hash is returned
 * @param {IAppConfigOrProperty<K>} [defaultValue] if provided, will default if key is not found in config
 * @returns {IAppConfigOrProperty<K>}
 * @memberof Configurator
 */
export const getConfig = function<K extends keyof IAppConfig | undefined>(
  key?: K,
  options: { useDefault?: boolean; default?: IAppConfigOrProperty<K> } = {}
): IAppConfigOrProperty<K> {
  // Ensure that the application configuration has been successfully cached
  if (!configLoaded) {
    throw new Error('Please ensure you have invoked the `load` method successfully prior to calling `getConfig`.');
  }

  return typeof key === 'string' && appConfig.hasOwnProperty(key as keyof IAppConfig)
    ? (deepClone(appConfig[key as keyof IAppConfig]) as IAppConfigOrProperty<K>)
    : options.useDefault
    ? (options.default as IAppConfigOrProperty<K>)
    : (deepClone(appConfig) as IAppConfigOrProperty<K>);
};

export default class Configurator extends Service implements IConfigurator {
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

  getConfig<K extends keyof IAppConfig | undefined>(
    key?: K,
    options: { useDefault?: boolean; default?: IAppConfigOrProperty<K> } = {}
  ): IAppConfigOrProperty<K> {
    return getConfig(key, options);
  }
}

/**
 * Syntactic sugar over Configurator. Instead of Configurator.getConfig('xxxx'),
 * you can do, config.xxxx which is a little bit shorter. Also it will return
 * undefined if the config is not found
 */
export const config: Partial<IAppConfig> = new Proxy<Partial<IAppConfig>>(appConfig, {
  /**
   * Proxy getter for Configurator
   * @param obj {IAppConfig}
   * @param prop {keyof IAppConfig}
   */
  get: function<K extends keyof IAppConfig>(_: IAppConfig, prop: K): IAppConfigOrProperty<K> {
    return getConfig(prop, {
      useDefault: true,
      default: undefined
    });
  }
});

/**
 * For testing purposes: sets a new config
 * TODO: META-12096 Deprecate ability to set mock config and rely Mirage configs / scenarios instead
 * Setting this is an anti-pattern akin to setting global state, makes tests brittle and causes random tests failures
 * Tests should utilize the stubService helper in Component(Integration) tests, and / or Mirage scenarios
 * in Application(Acceptance) tests
 * @see https://www.ember-cli-mirage.com/docs/testing/acceptance-tests#keeping-your-tests-focused
 * @see https://www.ember-cli-mirage.com/docs/testing/acceptance-tests#arrange-act-assert
 * @param config
 */
export const setMockConfig = (config?: Partial<IAppConfig>): void => {
  configLoaded = true;
  Object.assign(appConfig, config);
};

/**
 * For testing purposes: reset config state
 */
export const resetConfig = (): void => {
  configLoaded = false;
  appConfig = {};
};

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    configurator: Configurator;
  }
}
