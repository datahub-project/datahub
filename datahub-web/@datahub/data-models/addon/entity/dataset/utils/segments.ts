import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';

/**
 * Will return the dataset delimiter given a platform. For example HDFS uses '/' while the HIVE '.'
 * @param category Platform that we want to obtain the delimiter
 */
export const getDelimiter = async (category: string): Promise<string> => {
  const dataPlatforms: Array<IDataPlatform> = await readDataPlatforms();
  const currentDataPlatform = dataPlatforms.find(platform => platform.name === category);
  const { datasetNameDelimiter = '.' } = currentDataPlatform || {};
  return datasetNameDelimiter;
};

/**
 * Will return the expected prefix for the browse api given segments and a category
 * @param category current browse category
 * @param segments current segments without category
 */
export const getPrefix = async (category: string, segments: Array<string>): Promise<string> => {
  const filteredSegments = segments.filter(Boolean);
  const datasetNameDelimiter = await getDelimiter(category);
  const prefix =
    filteredSegments.length > 0
      ? `${datasetNameDelimiter === '/' ? '/' : ''}${filteredSegments.join(
          datasetNameDelimiter
        )}${datasetNameDelimiter}`
      : '';

  return prefix;
};
