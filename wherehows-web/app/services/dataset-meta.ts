import Service from '@ember/service';

/**
 * The dataset meta service can be used to share information between dataset route containers.
 * Used to share health score but can be expanded to other meta informations
 */
export default class DatasetMeta extends Service {
  healthScore: number;
}

declare module '@ember/service' {
  interface Registry {
    'dataset-meta': DatasetMeta;
  }
}
