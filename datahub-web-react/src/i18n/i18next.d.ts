import type enCommonActions from '@src/i18n/locales/en/common.actions.json';
import type enSettingsFeatures from '@src/i18n/locales/en/settings.features.json';
import type enSettingsPage from '@src/i18n/locales/en/settings.page.json';
import type enSettingsPosts from '@src/i18n/locales/en/settings.posts.json';
import type enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';
import type enSettingsTokens from '@src/i18n/locales/en/settings.tokens.json';

declare module 'i18next' {
    interface CustomTypeOptions {
        resources: {
            'common.actions': typeof enCommonActions;
            'settings.features': typeof enSettingsFeatures;
            'settings.page': typeof enSettingsPage;
            'settings.posts': typeof enSettingsPosts;
            'settings.preferences': typeof enSettingsPreferences;
            'settings.tokens': typeof enSettingsTokens;
        };
    }
}
