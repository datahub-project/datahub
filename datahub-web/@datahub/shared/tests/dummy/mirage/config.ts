import { Server, Request } from 'ember-cli-mirage';
import { setup } from '@datahub/shared/mirage-addon/mirage-config';

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
}
