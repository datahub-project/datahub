import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { getInstitutionalMemory, postInstitutionalMemory } from 'dummy/mirage/helpers/institutional-memory';
import { IFollowsAspect } from '@datahub/metadata-types/types/aspects/social-actions';

export default function(this: IMirageServer): void {
  this.get('/pokemons/:urn/institutionalmemory', getInstitutionalMemory);
  this.post('/pokemons/:urn/institutionalmemory', postInstitutionalMemory);
  // TODO Mirage for follows: https://jira01.corp.linkedin.com:8443/browse/META-11926
  this.get(
    '/api/v2/pokemons/pikachu:urn/follows',
    (): IFollowsAspect => ({
      followers: [
        { follower: { corpUser: 'aketchum' } },
        { follower: { corpUser: 'misty' } },
        { follower: { corpUser: 'brock' } }
      ]
    })
  );
}
