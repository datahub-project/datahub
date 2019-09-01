import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { getInstitutionalMemory, postInstitutionalMemory } from 'dummy/mirage/helpers/institutional-memory';

export default function(this: IMirageServer) {
  this.get('/pokemons/:urn/institutionalmemory', getInstitutionalMemory);
  this.post('/pokemons/:urn/institutionalmemory', postInstitutionalMemory);
}
