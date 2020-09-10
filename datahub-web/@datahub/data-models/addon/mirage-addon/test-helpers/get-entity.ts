import { HandlerFunction, Schema, Request } from 'ember-cli-mirage';

interface IGetEntityParams {
  entityType: keyof Schema;
  identifier: string;
}
export const getEntity: HandlerFunction = function(schema: Schema, request: Request) {
  const params: IGetEntityParams | undefined = (request.params as unknown) as IGetEntityParams;
  const db = schema[params?.entityType];
  const results = db.where({ urn: params?.identifier });

  return this.serialize(results.models[0]);
};
