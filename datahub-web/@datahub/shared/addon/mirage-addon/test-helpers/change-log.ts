import { HandlerFunction, ModelInstance } from 'ember-cli-mirage';

export const getChangeLog: HandlerFunction = (
  schema
): ModelInstance<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement> => {
  return schema.changeLogs.first();
};
