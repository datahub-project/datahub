import ApplicationInstance from '@ember/application/instance';
import { setCacheEnabled } from '@datahub/utils/api/fetcher';

/**
 * Will set the cache based on the env of the application which this addon is running
 */
export function initialize(appInstance: ApplicationInstance): void {
  const config = appInstance.resolveRegistration('config:environment') as Record<string, unknown>;
  setCacheEnabled(config.environment !== 'test');
}

export default {
  initialize
};
