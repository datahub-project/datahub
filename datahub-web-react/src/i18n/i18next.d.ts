// Imported for TypeScript type inference only — not bundled at runtime.
import type enCommonActions from '@src/i18n/locales/en/common.actions.json';

export type Resources = {
    'common.actions': typeof enCommonActions;
};

declare module 'i18next' {
    interface CustomTypeOptions {
        resources: Resources;
    }
}
