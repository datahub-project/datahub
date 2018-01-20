import Service from '@ember/service';
import { assert } from '@ember/debug';
import fetch from 'fetch';

/**
 * Deep clones a reference value provided. If the value is primitive, i.e. not immutable deepClone is an
 * identity function
 * @param {any} value the value to clone
 * @return value
 */
const deepClone = value => (typeof value === 'object' ? JSON.parse(JSON.stringify(value)) : value);

const appConfig = {};
const appConfigUrl = '/config';
let configLoaded = false;

export default Service.extend({
  /**
   * Fetches the application configuration object from the provided endpoint and augments the appConfig object
   * @return {Promise.<any>}
   */
  async load() {
    try {
      const { status, config } = await fetch(appConfigUrl).then(response => response.json());

      if (status === 'ok') {
        return (configLoaded = true) && Object.assign(appConfig, config);
      }

      return Promise.reject(new Error(`Configuration load failed with status: ${status}`));
    } catch (e) {
      configLoaded = false;

      return Promise.reject(e);
    }
  },

  /**
   * Returns the last saved configuration object if one was successfully retrieved
   * @param {String} key if provided, the value is returned with that key on the config hash is returned
   * @return {any}
   */
  getConfig: key => {
    assert('Please ensure you have invoked the `load` method successfully prior to calling `getConfig`.', configLoaded);

    // Ensure that the application configuration has been successfully cached
    if (configLoaded) {
      if (key) {
        return deepClone(appConfig[key]);
      }

      return deepClone(appConfig);
    }
  }
});
