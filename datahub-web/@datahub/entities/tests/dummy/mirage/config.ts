import { Server } from 'ember-cli-mirage';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { datasetsMirageConfig } from '@datahub/entities/mirage-addon/datasets-config';

export default function(this: Server): void {
  this.namespace = getApiRoot(ApiVersion.v2);

  datasetsMirageConfig(this);
}
