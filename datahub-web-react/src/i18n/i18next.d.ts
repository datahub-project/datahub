import type enCommonActions from '@src/i18n/locales/en/common.actions.json';
import type enEntityIdentity from '@src/i18n/locales/en/entity.identity.json';
import type enEntityOwnership from '@src/i18n/locales/en/entity.ownership.json';
import type enEntityViews from '@src/i18n/locales/en/entity.views.json';
import type enHomeV2 from '@src/i18n/locales/en/home.v2.json';
import type enHomeV3 from '@src/i18n/locales/en/home.v3.json';
import type enModules from '@src/i18n/locales/en/modules.json';
import type enSettingsFeatures from '@src/i18n/locales/en/settings.features.json';
import type enSettingsPage from '@src/i18n/locales/en/settings.page.json';
import type enSettingsPermissions from '@src/i18n/locales/en/settings.permissions.json';
import type enSettingsPosts from '@src/i18n/locales/en/settings.posts.json';
import type enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';
import type enSettingsTokens from '@src/i18n/locales/en/settings.tokens.json';
import type enSharedQueryBuilder from '@src/i18n/locales/en/shared.query-builder.json';

declare module 'i18next' {
    interface CustomTypeOptions {
        resources: {
            'common.actions': typeof enCommonActions;
            'entity.identity': typeof enEntityIdentity;
            'entity.ownership': typeof enEntityOwnership;
            'entity.views': typeof enEntityViews;
            'home.v2': typeof enHomeV2;
            'home.v3': typeof enHomeV3;
            modules: typeof enModules;
            'settings.features': typeof enSettingsFeatures;
            'settings.page': typeof enSettingsPage;
            'settings.permissions': typeof enSettingsPermissions;
            'settings.posts': typeof enSettingsPosts;
            'settings.preferences': typeof enSettingsPreferences;
            'settings.tokens': typeof enSettingsTokens;
            'shared.query-builder': typeof enSharedQueryBuilder;
        };
    }
}
