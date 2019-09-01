import { ApiStatus } from '@datahub/utils/api/shared';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

// TODO: META-9304 Move to @linkedin/shared

/**
 * Describes the interface for the IAppConfig object
 * @interface IAppConfig
 */
export interface IAppConfig {
  isInternal: boolean | void;
  isStagingBanner: boolean;
  isLiveDataWarning: boolean;
  showAdvancedSearch: boolean;
  shouldShowDatasetLineage: boolean;
  shouldShowDatasetHealth: boolean;
  // confidence threshold for filtering out higher quality suggestions
  suggestionConfidenceThreshold: number;
  // collection of links to external help resource pages
  wikiLinks: Record<string, string>;
  // Properties related to whether or not we want to show a change management banner to inform users that
  // the application is under some kind of maintenance/update
  showChangeManagement: boolean;
  changeManagementLink: string;
  // properties for an avatar entity
  userEntityProps: {
    aviUrlPrimary: string;
    aviUrlFallback: string;
  };
  // Flag indicating if the Feature Entity is available for current environment
  showFeatures: boolean;
  // browse and search ump entities
  showUmp: boolean;
  // Show Flows in user menu
  showUmpFlows: boolean;
  // Use new browse API for datasets
  useNewBrowseDataset: boolean;
  // Show lineage graph in the dataset relationships tab
  showLineageGraph: boolean;
  showComplianceBeta: boolean;
  showInstitutionalMemory: boolean;
  tracking: {
    isEnabled: boolean;
    trackers: {
      piwik: {
        piwikSiteId: number;
        piwikUrl: string;
      };
    };
  };
  // Configuration properties for the JIT ACL access request feature
  jitAcl: {
    // Maximum allowed duration from now per FabricType
    maxDuration: {
      PROD: string;
      EI: string;
      CORP: string;
    };
    // Lists the DatasetPlatforms that are supported for JIT ACL requests
    whitelist: Array<DatasetPlatform>;
    // Email contact for issues with JIT ACL service and endpoint
    contact: string;
  };
}

/**
 * Describes the interface for the json response when a GET request is made to the
 * configurator endpoint
 * @interface IConfiguratorGetResponse
 */
export interface IConfiguratorGetResponse {
  status: ApiStatus;
  config: IAppConfig;
}
