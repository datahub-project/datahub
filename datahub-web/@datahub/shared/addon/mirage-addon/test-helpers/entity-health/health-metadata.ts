import { HandlerFunction, Schema, Request } from 'ember-cli-mirage';

export const getEntityHealth: HandlerFunction = (schema: Schema, { params: { id = 1 } }: Request) => ({
  health: schema.db.entityHealths.firstOrCreate({ id: String(id) })
});
