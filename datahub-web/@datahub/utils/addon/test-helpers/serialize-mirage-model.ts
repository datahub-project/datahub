import { Server } from 'ember-cli-mirage';
import { IMirageSchemaRegistry } from 'ember-cli-mirage/types/registries/schema';

/**
 * Serializes the Mirage Schema models from the instance of the supplied Mirage Server
 * Useful in Ember Rendering or Unit tests that need to extract a value from the Mirage DB without making
 * a request to a Mirage endpoint
 * @template M
 * @param {M} modelName the name of the Mirage Model to serialize instances
 * @param {Server} server reference to the Mirage server instance, typically this.server in a MirageTestContext
 */
export const getSerializedMirageModel = <K extends keyof IMirageSchemaRegistry>(
  modelName: K,
  server: Server
): Array<IMirageSchemaRegistry[K]> => {
  const serializer = server.serializerOrRegistry.serializerFor(modelName);
  const models = server.schema[modelName as string].all();

  return serializer.serialize(models) as Array<IMirageSchemaRegistry[K]>;
};
