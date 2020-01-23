import Service from '@ember/service';
import { set } from '@ember/object';
import {
  DatasetPlatformsList,
  createDatasetPlatformsList
} from '@datahub/data-models/entity/dataset/modules/platforms-list';

/**
 * The DatasetsCore service connects us to a more shared data layer between datasets, such as compliance
 * data types or platforms API that may need to be called repeatedly between various dataset related components
 * and instances of those components. As such, we use this service to read such static information once
 * and cache its value here.
 */
export default class DatasetsCoreService extends Service {
  readonly dataPlatforms?: DatasetPlatformsList;

  /**
   * If we already have data platforms available, then return it. Otherwise, await a new instance of the
   * DatasetPlatforms list
   */
  async getDataPlatforms(): Promise<DatasetPlatformsList> {
    const { dataPlatforms } = this;

    if (dataPlatforms) {
      return dataPlatforms;
    }

    const platformsList = await createDatasetPlatformsList();
    if (platformsList) {
      set(this, 'dataPlatforms', platformsList);
    }
    return platformsList;
  }
}

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'datasets-core': DatasetsCoreService;
  }
}
