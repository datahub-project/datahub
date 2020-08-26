import { IBaseEntity } from '@datahub/metadata-types/types/entity/index';
import { FeatureStatusType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-status-type';
import { FeatureTierType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-tier-type';

/**
 * Specification of a generic Feature entity
 * https://jarvis.corp.linkedin.com/codesearch/result/?name=FeatureEntity.pdsc&path=metadata-models%2Fmetadata-models%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2Fmetadata%2Fentity&reponame=multiproducts%2Fmetadata-models
 */
export interface IFeatureEntity extends IBaseEntity {
  // Urn for the feature. Urn is constructed from two parts, 1. namespace (which is the feature multiproduct name) and 2. feature name
  urn: string;
  // Feature name
  name?: string;
  // Namespace of the feature, e.g. jymbii, waterloo, careers etc
  namespace?: string;
  // A union of all supported feature base entity types
  baseEntity?: string;
  // Classification of the feature, e.g. Characteristic, Activity etc
  classification?: string;
  // Category of the feature, like InmailSent, CompanyStandardizedData, AdsClickCampaignType
  category?: string;
  // Status for the feature
  status?: FeatureStatusType;
  // Tier for the feature
  tier?: FeatureTierType;
}
