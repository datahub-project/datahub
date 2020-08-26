import { Server } from 'ember-cli-mirage';
import { setup } from '@datahub/shared/mirage-addon/mirage-config';

/**
 * Default handler for Mirage config
 * @export
 * @param {Server} this the Mirage server instance
 */
export default function(this: Server): void {
  setup(this);
}
