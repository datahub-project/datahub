import Ember from 'ember';
import fetch from 'ember-network/fetch';

const { Service, assert } = Ember;

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
   * @throws
   */
  async load() {
    try {
      const config = await fetch(appConfigUrl).then(response => response.json());
      configLoaded = true;

      return Object.assign(appConfig, config);
    } catch (e) {
      configLoaded = false;

      throw e;
    }
  },

  /**
   * Returns the last saved configuration object if one was successfully retrieved
   * @param {String} key if provided and the key attribute is present on the config hash, the value is returned
   * @return {any|null}
   */
  getConfig: key => {
    assert('Please ensure you have invoked the load method successfully prior to calling getConfig', configLoaded);

    // Ensure that the application configuration has been successfully cached
    if (configLoaded) {
      if (key in appConfig) {
        return deepClone(appConfig[key]);
      }

      return deepClone(appConfig);
    }

    return null;
  }
});
