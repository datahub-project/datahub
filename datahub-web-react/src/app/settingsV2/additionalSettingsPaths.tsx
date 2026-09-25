import { SettingsPath } from '@app/settingsV2/types';

/**
 * Extension point for deployments that ship settings pages DataHub does not build in.
 *
 * It is a module of its own so that such a deployment replaces this one file rather than
 * editing the built-in list in `settingsPaths.tsx` — an edit in the middle of that array
 * collides with every subsequent change to it. DataHub itself ships no additional pages,
 * so the list here is empty by design.
 */
export const ADDITIONAL_PATHS: SettingsPath[] = [];
