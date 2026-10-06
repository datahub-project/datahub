/**
 * Shared feature flag configurations for Playwright tests.
 * Always include GLOBAL_FEATURE_FLAGS when setting test-specific flags.
 */

export const GLOBAL_FEATURE_FLAGS = {
  showProductUpdates: false,
} as const;

export const THEME_V2_FLAGS = {
  ...GLOBAL_FEATURE_FLAGS,
  themeV2Enabled: true,
  themeV2Default: true,
  showNavBarRedesign: true,
} as const;
