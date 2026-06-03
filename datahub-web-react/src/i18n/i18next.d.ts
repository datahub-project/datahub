import type enCommonActions from '@src/i18n/locales/en/common.actions.json';
import type enCommonFeedback from '@src/i18n/locales/en/common.feedback.json';
import type enCommonLabels from '@src/i18n/locales/en/common.labels.json';
import type enEntityIdentity from '@src/i18n/locales/en/entity.identity.json';
import type enEntityOwnership from '@src/i18n/locales/en/entity.ownership.json';
import type enEntityProfileAccess from '@src/i18n/locales/en/entity.profile.access.json';
import type enEntityProfileDocumentation from '@src/i18n/locales/en/entity.profile.documentation.json';
import type enEntityProfileIncident from '@src/i18n/locales/en/entity.profile.incident.json';
import type enEntityProfileQueries from '@src/i18n/locales/en/entity.profile.queries.json';
import type enEntityProfileSchema from '@src/i18n/locales/en/entity.profile.schema.json';
import type enEntityProfileStats from '@src/i18n/locales/en/entity.profile.stats.json';
import type enEntityProfileValidations from '@src/i18n/locales/en/entity.profile.validations.json';
import type enEntityProfileView from '@src/i18n/locales/en/entity.profile.view.json';
import type enEntityViews from '@src/i18n/locales/en/entity.views.json';
import type enGovernanceDomain from '@src/i18n/locales/en/governance.domain.json';
import type enHomeV2 from '@src/i18n/locales/en/home.v2.json';
import type enHomeV3 from '@src/i18n/locales/en/home.v3.json';
import type enMisc from '@src/i18n/locales/en/misc.json';
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
            'common.feedback': typeof enCommonFeedback;
            'common.labels': typeof enCommonLabels;
            'entity.identity': typeof enEntityIdentity;
            'entity.ownership': typeof enEntityOwnership;
            'entity.profile.documentation': typeof enEntityProfileDocumentation;
            'entity.profile.incident': typeof enEntityProfileIncident;
            'entity.profile.validations': typeof enEntityProfileValidations;
            'entity.profile.access': typeof enEntityProfileAccess;
            'entity.profile.queries': typeof enEntityProfileQueries;
            'entity.profile.schema': typeof enEntityProfileSchema;
            'entity.profile.stats': typeof enEntityProfileStats;
            'entity.profile.view': typeof enEntityProfileView;
            'entity.views': typeof enEntityViews;
            'governance.domain': typeof enGovernanceDomain;
            'home.v2': typeof enHomeV2;
            'home.v3': typeof enHomeV3;
            misc: typeof enMisc;
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
