import type { AiSettingsTab } from '@app/settingsV2/platform/PlatformAiSettings.types';

/**
 * Extension point for deployments that ship AI settings tabs DataHub does not build in.
 *
 * It is a module of its own so that such a deployment replaces this one file rather than
 * editing the built-in tabs in `PlatformAiSettings.tsx`, the same way `additionalSettingsPaths.tsx`
 * extends the settings pages. It is a hook, not a list, so those tabs can read translations,
 * feature flags and privileges and keep their own state (a Create button and its modal, say).
 * Each tab's `order` places it among the built-in ones (MCP Servers is 20). DataHub itself ships
 * no additional tabs, so the list here is empty by design.
 */
export function useAdditionalAiSettingsTabs(): AiSettingsTab[] {
    return [];
}
