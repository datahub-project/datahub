import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';
import { uniq } from 'lodash';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { PlatformsWithSlash } from '@datahub/data-models/entity/dataset/utils/urn';

/**
 * Will group by dataset name (path) for example:
 * 1: /a/b/asd
 * 2: /a/b/qwe
 * 3: /c/dsds
 *
 * for root:
 * /a/
 * /c/
 *
 * for /a/:
 * /a/b/
 *
 * for /a/b/:
 * /a/b/asd
 * /a/b/qwe
 */
export const getBrowsePlatform = function(
  this: IFunctionRouteHandler,
  { datasetViews }: IMirageWherehowsDBs,
  request: IMirageRequest<{}, { platform: DatasetPlatform; prefix?: string }>
): Array<string> {
  const { platform = DatasetPlatform.HDFS, prefix = '' } = request.params || {};
  const separator = PlatformsWithSlash[platform] ? '/' : '.';
  const datasets = datasetViews
    .where({ platform })
    .models.filter(dataset => (prefix ? dataset.name.indexOf(prefix) === 0 : true))
    .map(model => {
      const nameWithoutPrefix = model.name.replace(prefix || '', '');
      const tokens = nameWithoutPrefix.split(separator).filter((token: string) => !!token);
      const lastSeparator = tokens.length === 1 ? '' : separator; // is the last part of the path (datasetname)
      return `${prefix || separator}${tokens[0]}${lastSeparator}`;
    });

  return uniq(datasets);
};
