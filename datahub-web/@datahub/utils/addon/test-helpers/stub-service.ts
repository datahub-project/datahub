import Service from '@ember/service';
import { TestContext } from 'ember-test-helpers';
import { getContext } from '@ember/test-helpers';

/**
 * Registers a stub service for component integration testing
 * @param {string} name the name of the service without the service: prefix e.g. 'location-data'
 * @param {*} [props={}] properties to be stubbed on the service interface
 */
export const stubService = (name: string, props = {}): void => {
  const serviceStub = Service.extend(props);
  const { owner } = getContext() as TestContext; // getContext return type is object, assert TestContext

  owner.register(`service:${name}`, serviceStub);
};
