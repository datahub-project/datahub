import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageInstitutionalMemorySchema } from '../../../tests/dummy/mirage/types/schema';
import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';

/**
 * Gets all the institutional memory objects available
 */
export const getInstitutionalMemory = function(
  this: IFunctionRouteHandler,
  schema: IMirageInstitutionalMemorySchema
): { elements: Array<IInstitutionalMemory> } {
  return { elements: schema.db.institutionalMemories };
};

/**
 * Posts a snapshot of the institutional memory objects back onto the db
 */
export const postInstitutionalMemory = function(
  this: IFunctionRouteHandler,
  schema: IMirageInstitutionalMemorySchema,
  req: IMirageRequest
): void {
  const requestBody: { elements: Array<IInstitutionalMemory> } = JSON.parse(req.requestBody);

  schema.db.institutionalMemories.remove();
  requestBody.elements.forEach(memory => {
    if (!memory.createStamp) {
      memory.createStamp = { actor: 'pikachu', time: Date.now() };
    }

    schema.db.institutionalMemories.insert(memory);
  });
};
