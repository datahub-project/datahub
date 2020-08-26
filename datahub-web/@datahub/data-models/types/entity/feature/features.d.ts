import { IFeatureEntity } from '@datahub/metadata-types/types/entity/feature/feature-entity';
import { FeatureStatusType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-status-type';

/**
 * Describes the interface for a feature category object
 * @export
 * @interface IFeatureNode
 */
export type IFeatureNode = Partial<IFeatureEntity> & { urn: IFeatureEntity['urn'] | 'urn:li:feature:(namespace,name)' };

/**
 * Describes the response shape returned when a requests is made for a list of features and feature categories
 * @export
 * @interface IReadFeaturesResponse
 */
export interface IReadFeatureHierarchyResponse {
  // List if Feature nodes contained within this hierarchy
  elements: Array<IFeatureNode>;
  start: number;
  count: number;
  // Total number of nodes available are this hierarchy
  total: number;
}

/**
 * Path segments / category + prefix to a Feature node mapping to baseEntity, classification, category
 */
export type FeatureSegments = [string, undefined | string, undefined | string];

/**
 * Search response for feature
 */
export interface ISearchFeatureEntity {
  anchors: Array<string>;
  availability: string;
  baseEntity: string;
  browsePaths: Array<string>;
  category: string;
  classification: string;
  description: string;
  documentationLink: string;
  multiproduct: string;
  multiproductVersion: string;
  name: string;
  namespace: string;
  owners: Array<string>;
  path: string;
  sources: Array<string>;
  status: FeatureStatusType;
  tier: string;
  urn: string;
}
