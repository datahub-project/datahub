import { datasetsMirageConfig } from '@datahub/datasets-core/mirage-addon/datasets-config';
import { Server } from 'ember-cli-mirage';

export default function(this: Server): void {
  datasetsMirageConfig(this);
}
