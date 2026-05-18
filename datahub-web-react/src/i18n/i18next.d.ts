// Imported for TypeScript type inference only — not bundled at runtime.
import type enCommonActions from '@src/i18n/locales/en/common.actions.json';
import type enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';

export type Resources = {
    'common.actions': typeof enCommonActions;
    'settings.preferences': typeof enSettingsPreferences;
};

declare module 'i18next' {
    interface CustomTypeOptions {
        resources: Resources;
    }
}
