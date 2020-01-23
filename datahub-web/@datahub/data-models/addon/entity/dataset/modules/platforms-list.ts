import { set } from '@ember/object';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

/**
 * A simple wrapper around the reader for a compliance data types list that helps us provide the read from
 * the data layer to the consuming application
 */
export class DatasetPlatformsList {
  /**
   * The read list for the compliance data types, which are used to determine what options are available
   * for annoations of a dataset's fields
   * @type {Array<IDataPlatform>}
   */
  readonly list?: Array<IDataPlatform>;

  /**
   * Caches any search for a platform so that subsequent searches don't require scanning through the
   * array list, using the platform code name as the key
   * @type {Partial<Record<DatasetPlatform, IDataPlatform>>}
   */
  private platformHashedByName: Partial<Record<DatasetPlatform, IDataPlatform>> = {};

  /**
   * Read function to fetch the compliance data types from the API layer
   */
  readPlatformsList(): Promise<Array<IDataPlatform>> {
    return readDataPlatforms();
  }

  /**
   * Gets the object of information related to a platform by its name. This is a function to
   * abstract the details of finding the platform detail to the data layer.
   * @param {DatasetPlatform} name - the code name for a data platform
   */
  readPlatform(name: DatasetPlatform): IDataPlatform | undefined {
    const { platformHashedByName, list = [] } = this;
    const platformByName = platformHashedByName[name];

    if (platformByName) {
      return platformByName;
    }

    const platformFromList = list.find(platform => platform.name === name);

    if (!platformFromList) {
      return;
    }

    platformHashedByName[name] = platformFromList;
    return platformFromList;
  }
}

/**
 * Custom factory for the data types list factory, useful as this does not follow the typical creator
 * for a base entity.
 */
export const createDatasetPlatformsList = async (): Promise<DatasetPlatformsList> => {
  const platformsList = new DatasetPlatformsList();
  const platforms = await platformsList.readPlatformsList();

  set(platformsList, 'list', platforms);
  return platformsList;
};
