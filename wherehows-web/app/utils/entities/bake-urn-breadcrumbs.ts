import { DatasetPlatform } from 'wherehows-web/constants';
import { datasetUrnRegexLI } from 'wherehows-web/utils/validators/urn';

/**
 * Describes the interface for a breadcrumb object
 * @interface IDatasetBreadcrumb
 */
export interface IDatasetBreadcrumb {
  crumb: string;
  platform: DatasetPlatform;
  prefix: string | void;
}

/**
 * Takes a urn string and parses it into an array of breadcrumb objects with crumb, and query params, prefix and/or platform as
 * properties.
 * Hierarchy is implied in element ordering
 * @param {string} urn the dataset urn in li format
 * @returns {Array<IDatasetBreadcrumb>}
 */
export default (urn: string): Array<IDatasetBreadcrumb> => {
  const liDatasetUrn = datasetUrnRegexLI.exec(urn);
  const breadcrumbs: Array<IDatasetBreadcrumb> = [];

  if (liDatasetUrn) {
    const [, platform, segments = ''] = liDatasetUrn;
    const isHdfs = String(platform).toLowerCase() === DatasetPlatform.HDFS;
    // For HDFS drop leading slash
    const hierarchy = isHdfs ? segments.split('/').slice(1) : segments.split('.');

    return [platform, ...hierarchy].reduce((breadcrumbs: Array<IDatasetBreadcrumb>, crumb: string, index) => {
      if (crumb) {
        let prefix: void | string;

        // List isn't empty an a previous crumb exists
        if (index) {
          const previousCrumb = breadcrumbs[index - 1];
          const { prefix: previousPrefix = '' } = previousCrumb;
          // if hdfs, precede with slash, otherwise trailing period
          prefix = isHdfs ? `${previousPrefix}/${crumb}` : `${previousPrefix}${crumb}.`;
        }

        return [
          ...breadcrumbs,
          {
            crumb,
            prefix,
            platform: <DatasetPlatform>platform
          }
        ];
      }

      return breadcrumbs;
    }, breadcrumbs);
  }

  return breadcrumbs;
};
