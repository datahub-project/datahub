import Service from '@ember/service';
import { DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';

/**
 * The data models service is meant to provide access to the models available in data models. The
 * reason this exists is to allow for the ability to use said models without a direct import of
 * the model types to the consuming addons. This makes it easier to separate our open source and
 * internal logic per model and implementation of that model
 */
export default class DataModelsService extends Service {
  /**
   * Returns a given data model class in order to be instantiated by the consumer.
   * @example
   * usage =>
   * const datasetModel = DataModelsService.getModel('dataset');
   * const someInstance = new datasetModel('urn', optionalParams);
   *
   * @param modelKey - key that determines which model we should be returning
   */
  getModel<K extends DataModelName>(modelKey: K): typeof DataModelEntity[K] {
    return DataModelEntity[modelKey];
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // This is a core ember thing
  //eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'data-models': DataModelsService;
  }
}
