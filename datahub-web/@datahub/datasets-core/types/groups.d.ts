/**
 * Describes the shape of an individual DatasetGroup result
 * @interface IDatasetGroupAPIResponse
 */
export interface IDatasetGroupAPIResponse {
  urn: string;
}

/**
 * Definition of the how the DatasetGroup API response looks like
 * @type IDatasetGroupsAPIResponse
 */
export type IDatasetGroupsAPIResponse = Array<IDatasetGroupAPIResponse>;
