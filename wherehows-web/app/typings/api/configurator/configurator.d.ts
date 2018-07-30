import { ApiStatus } from 'wherehows-web/utils/api';
import { DatasetPlatform } from 'wherehows-web/constants';

/**
 * Describes the interface for the IAppConfig object
 * @interface IAppConfig
 */
interface IAppConfig {
  isInternal: boolean | void;
  JitAclAccessWhitelist: Array<DatasetPlatform> | void;
  shouldShowDatasetLineage: boolean;
  shouldShowDatasetHealth: boolean;
  // confidence threshold for filtering out higher quality suggestions
  suggestionConfidenceThreshold: number;
  tracking: {
    isEnabled: boolean;
    trackers: {
      piwik: {
        piwikSiteId: number;
        piwikUrl: string;
      };
    };
  };
}

/**
 * Describes the interface for the json response when a GET request is made to the
 * configurator endpoint
 * @interface IConfiguratorGetResponse
 */
interface IConfiguratorGetResponse {
  status: ApiStatus;
  config: IAppConfig;
}

export { IAppConfig, IConfiguratorGetResponse };
