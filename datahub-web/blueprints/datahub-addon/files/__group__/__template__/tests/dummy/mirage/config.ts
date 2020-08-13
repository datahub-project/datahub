import { Server } from 'ember-cli-mirage';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';

export default function(this: Server): void {
  this.namespace = getApiRoot(ApiVersion.v2);
}
