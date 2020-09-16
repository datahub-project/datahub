import { Server, Request, HandlerFunction } from 'ember-cli-mirage';
import { setup } from '@datahub/shared/mirage-addon/mirage-config';
import {
  getInstitutionalMemory,
  postInstitutionalMemory
} from '@datahub/shared/mirage-addon/helpers/institutional-memory';
import { IFollowsAspect } from '@datahub/metadata-types/types/aspects/social-actions';

/**
 * Default handler for Mirage config
 * @export
 * @param {Server} this the Mirage server instance
 */
export default function(this: Server): void {
  setup(this);

  this.get('/features/:urn', function(_schema: unknown, request: Request) {
    return { urn: request.params.urn };
  });

  this.get('/features/snapshots/:urns', function(_schema: unknown, request: Request) {
    return (request.params.urns as string).split(';').map((urn: string) => ({ urn }));
  });

  this.namespace = '';
  this.get('/pokemons/:urn/institutionalmemory', (getInstitutionalMemory as unknown) as HandlerFunction);
  this.post('/pokemons/:urn/institutionalmemory', (postInstitutionalMemory as unknown) as HandlerFunction);
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
