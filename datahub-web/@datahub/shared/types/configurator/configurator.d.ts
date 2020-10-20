import { ITrackingConfig } from '@datahub/shared/types/configurator/tracking';
import { ApiStatus } from '@datahub/utils/api/shared';
import Service from '@ember/service';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { AuthenticationType } from '@datahub/shared/constants/authentication/auth-type';

/**
 * Describes the interface for the configuration endpoint response object.
 * These values help to determine how the app behaves at runtime for example feature flags and feature configuration properties
 * @interface IAppConfig
 */
export interface IAppConfig {
  isInternal: boolean | void;
  // Email address for application feedback / support
  applicationSupportEmail: string;
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
  isLiveDataWarning: boolean;
  isStagingBanner: boolean;
  // Attributes for analytics tracking features within the application
  tracking: ITrackingConfig;
  // Whether to show people entity elements
  showPeople: boolean;
  showDataConstructChangeManagement: boolean;
  // Flag guard for new lineage developments
  showLineageV3: boolean;
  // Configuration properties for the JIT ACL access request feature
  jitAcl: {
    // Maximum allowed duration from now per FabricType
    maxDuration: {
      PROD: string;
      EI: string;
      CORP: string;
    };
    // Lists the DatasetPlatforms that are supported for JIT ACL requests
    allowList: Array<DatasetPlatform>;
    // Email contact for issues with JIT ACL service and endpoint
    contact: string;
  };

  // Configurations for a custom banner to display on the application based on our config source
  customBanner: {
    showCustomBanner: boolean;
    content: string;
    type: string;
    icon: string;
    link: string;
  };

  // confidence threshold for filtering out higher quality suggestions
  suggestionConfidenceThreshold: number;

  // Whether or not to show the social actions features
  showSocialActions: boolean;

  // Whether or not to show social action features that are currently in progress
  showPendingSocialActions: boolean;

  // Whether or not to show recommendations
  showDatasetRecommendations: boolean;

  // Guards against toggling the redesigned implementation for Entity Health
  useVersion3Health: boolean;

  // Whether or not to show the top consumers feature
  showTopConsumers: boolean;

  // Whether or not to use v2 dataset search API
  useDatasetV2Search: boolean;

  // Specifies the user authentication workflow the application should use for login
  authenticationWorkflow: AuthenticationType;

  // Whether or not to show our foxie virtual assistant contents
  showFoxie?: boolean;

  // Entity feature config targets to be used when determining configurations for specific entities
  entityFeatureConfigTargets?: Record<string, Array<string>>;
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

/**
 * Conditional type alias for getConfig return type, if T is assignable to a key of
 * IAppConfig, then return the property value, otherwise returns the  IAppConfig object
 */
export type IAppConfigOrProperty<T, K extends IAppConfig = IAppConfig> = T extends keyof K
  ? K[T]
  : T extends undefined
  ? K
  : never;

export interface IConfigurator<T extends IAppConfig = IAppConfig> extends Service {
  getConfig<K extends keyof T | undefined>(
    key?: K,
    options?: { useDefault?: boolean; default?: IAppConfigOrProperty<K, T> }
  ): IAppConfigOrProperty<K, T>;
}
