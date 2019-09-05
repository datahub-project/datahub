/**
 * Describes the interface for subset of the tracking endpoint response object.
 * These values help to determine how tracking behaves in the application
 * @interface ITrackingConfig
 */
export interface ITrackingConfig {
  // Flag indicating that tracking should be enabled or not
  isEnabled: boolean;
  // Map of available trackers and configuration options per tracker
  trackers: {
    // Properties for Piwik analytics service tracking
    piwik: {
      // Website identifier for piwik tracking, used in setSideId configuration
      piwikSiteId: number;
      // Specifies the URL where the Matomo / Piwik tracking code is located
      piwikUrl: string;
    };
  };
}
