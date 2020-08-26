import { HandlerFunction } from 'ember-cli-mirage';

// Entity urn used to test entity feature onfigs
export const testUrn = 'urn:li:dataset:(urn:li:dataPlatform:hive,tracking.pageviewevent,PROD)';

// Feature target used to test entity feature configs
export const testTarget = 'appworx-deprecation';

// Gets the entity configs in the mock backend for entity configs
// TODO META-11235: Allow for entity feature configs container to batch targets
// This implementation currently reflects the behavior of the midtier, which only has one available target that returns a boolean instead of configs
export const getEntityConfigs: HandlerFunction = (): boolean => {
  // TODO META-11247: Fix issue with mirage not importing model into data portal
  return true;
};
