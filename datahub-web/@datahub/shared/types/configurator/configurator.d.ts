import { ITrackingConfig } from '@datahub/shared/types/configurator/tracking';
import Service from '@ember/service';
import { ApiStatus } from '@datahub/utils/api/shared';

/**
 * Describes the interface for the configuration endpoint response object.
 * These values help to determine how the app behaves at runtime for example feature flags and feature configuration properties
 * @interface IAppConfig
 */
export interface IAppConfig {
  // Attributes for analytics tracking features within the application
  tracking: ITrackingConfig;
  showLineageGraph: boolean;
  useNewBrowseDataset: boolean;
  isInternal: boolean;
  userEntityProps: {
    aviUrlPrimary: string;
    aviUrlFallback: string;
  };
  showChangeManagement: boolean;
  changeManagementLink: string;
  wikiLinks: Record<string, string>;
  isStagingBanner: boolean;
  isLiveDataWarning: boolean;
  shouldShowDatasetLineage: boolean;
  showInstitutionalMemory: boolean;
  showPeople: boolean;
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
export type IAppConfigOrProperty<T> = T extends keyof IAppConfig
  ? IAppConfig[T]
  : T extends undefined
  ? IAppConfig
  : never;

export interface IConfigurator extends Service {
  getConfig<K extends keyof IAppConfig | undefined>(
    key?: K,
    options?: { useDefault?: boolean; default?: IAppConfigOrProperty<K> }
  ): IAppConfigOrProperty<K>;
}
