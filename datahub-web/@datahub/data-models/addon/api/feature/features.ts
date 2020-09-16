import { featureUrlRoot } from '@datahub/data-models/api/feature/index';
import { ApiVersion } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { IReadFeatureHierarchyResponse } from '@datahub/data-models/types/entity/feature/features';
import buildUrl from '@datahub/utils/api/build-url';
import { FeatureStatusType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-status-type';

/**
 * Parameters for querying the Features Entity GET endpoint
 * @interface IReadFeaturesParameters
 */
interface IReadFeaturesHierarchyParameters {
  baseEntity?: string;
  classification?: string;
  category?: string;
  // Current status of the feature
  status?: FeatureStatusType;
}

/**
 * Constructs the base url for feature platforms
 */
const featuresUrl = (params: IReadFeaturesHierarchyParameters): string =>
  buildUrl(featureUrlRoot(ApiVersion.v2), params);

/**
 * Queries the Feature platforms endpoint to get the list of feature platforms
 */
export const readFeatureHierarchy = (
  params: IReadFeaturesHierarchyParameters
): Promise<IReadFeatureHierarchyResponse> => {
  // Only features that are in status FeatureStatusType.Published are available for browsing, adds a default status flag
  // If a specific status is specified in params, that value will take precedence over default below
  params = { status: FeatureStatusType.Published, ...params };

  return getJSON<IReadFeatureHierarchyResponse>({ url: featuresUrl(params) });
};
