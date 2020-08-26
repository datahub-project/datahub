import { HandlerFunction, ModelInstance } from 'ember-cli-mirage';

// Gets the top consumers aspect in the mirage scenario for top consumers
export const getTopConsumers: HandlerFunction = (schema): ModelInstance<Com.Linkedin.Common.EntityTopUsage> => {
  return schema.topConsumers.first();
};
