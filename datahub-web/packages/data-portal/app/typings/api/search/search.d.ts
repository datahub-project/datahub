import { ApiStatus } from '@datahub/utils/api/shared';
import { IDataset } from 'wherehows-web/typings/api/datasets/dataset';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { DataModelName } from '@datahub/data-models/constants/entity';

/**
 * Backend search expected parameters
 */
export interface ISearchApiParams {
  keyword: string;
  entity: DataModelName;
  page: number;
  facets: Record<string, Array<string>>;
  // Used to accomodate for the wording expected from the API side
  type?: DataModelName;
}

/**
 * Describes the interface for the result property found on the search endpoint response object
 * @interface ISearchResult
 */
interface ISearchResult {
  // Number of results for the search query
  count: number;
  // List of datasets matching the search query
  data: Array<IDataset>;
  // The category grouping for the search query results
  category: string;
  // Properties to group search results by number per Dataset data origins / fabrics
  groupbydataorigin: Partial<Record<FabricType, number>>;
  // Backwards compatible api for groupbydataorigin
  groupbyfabric: Partial<Record<FabricType, number>>;
  // Property to group search results by number per Dataset platforms / source
  groupbyplatform: Partial<Record<DatasetPlatform, number>>;
  // Backwards compatible api for groubyfabric
  groupbysource: Partial<Record<DatasetPlatform, number>>;
  // Items to be rendered per page of dataset result
  itemsPerPage: number;
  // The user entered keywords in the query that returned the result
  keywords: string;
  // The current page of the search results with datasets
  page: number;
  // Search results source
  source: null | string;
  // The total number of pages that can be rendered for this result, should match Math.ceil(count / itemsPerPage)
  totalPages: number;
}

/**
 * Backend search expected response
 */
export interface ISearchResponse {
  // Api response status code
  status: ApiStatus;
  // The result of the search query
  result: ISearchResult | null;
}
