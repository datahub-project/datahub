import type enCommonActions from '@src/i18n/locales/en/common.actions.json';
import type enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';

declare module 'i18next' {
    interface CustomTypeOptions {
        resources: {
            'common.actions': typeof enCommonActions;
            'settings.preferences': typeof enSettingsPreferences;
        };
    }
}
