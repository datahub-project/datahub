import { readPlatforms } from '@datahub/data-models/api/dataset/platform';
import {
  isDatasetSegment,
  isDatasetIdentifier,
  getPlatformFromString,
  isDatasetPlatform
} from '@datahub/data-models/entity/dataset/utils/platform';
import { getDelimiter } from '@datahub/data-models/entity/dataset/utils/segments';
import isUrn, { buildDatasetLiUrn } from '@datahub/data-models/entity/dataset/utils/urn';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

/**
 * Will get the last part of the name of a dataset. For example:
 * input: /a/b/asd output: asd
 * @param node
 * @param separator
 */
const getDisplayName = (node: string, separator: string): string => {
  const parts = node.split(separator).filter(Boolean);
  return parts[parts.length - 1];
};

/**
 * Interim interface until dataset readcategories is no longer needed
 * TODO META-8863
 */
interface IEntityOrCategory {
  entityUrn?: string;
  displayName: string;
  segments?: Array<string>;
}

/**
 * Read and parse categories from api for each entity
 * TODO META-8863
 */
export const readCategories = async (category: string, prefix: string): Promise<Array<IEntityOrCategory>> => {
  const datasetNameDelimiter = await getDelimiter(category);
  const platforms: Array<IEntityOrCategory> = (await readPlatforms({ platform: category, prefix })).map(
    (node): IEntityOrCategory => {
      const isSegment = isDatasetSegment(node);
      const isPlatform = isDatasetPlatform(node);
      const isEntity = isDatasetIdentifier(node);
      const platformName = getPlatformFromString(node) || '';
      // entity comes as abook so, we need to transform it into urn:li:dataset:(urn:li:dataPlatform:ambry,abook,PROD)
      const entityUrn = isEntity
        ? isUrn(node as string)
          ? (node as string)
          : buildDatasetLiUrn(category as DatasetPlatform, node)
        : undefined;

      const displayName = isPlatform ? platformName || '' : getDisplayName(node, datasetNameDelimiter);
      const segments = isSegment ? [category, ...node.split(datasetNameDelimiter)] : isPlatform ? [platformName] : [];

      return {
        entityUrn,
        displayName,
        segments: segments.filter(Boolean)
      };
    }
  );

  return platforms;
};
