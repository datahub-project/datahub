import { HandlerFunction, Schema, Request } from 'ember-cli-mirage';
import { pluralize } from 'ember-inflector';

interface IGetEntityParams {
  entityType: keyof Schema;
  identifier: string;
}
export const getEntity: HandlerFunction = function(schema: Schema, request: Request) {
  const params: IGetEntityParams | undefined = (request.params as unknown) as IGetEntityParams;
  const db = schema[params?.entityType] || schema[pluralize((params?.entityType as string) || '')];
  const results = db.where({ urn: params?.identifier });

  return this.serialize(results.models[0]);
};
