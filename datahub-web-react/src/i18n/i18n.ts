import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import deCommonActions from '@src/i18n/locales/de/common.actions.json';
import deSettingsPreferences from '@src/i18n/locales/de/settings.preferences.json';
import enCommonActions from '@src/i18n/locales/en/common.actions.json';
import enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';

i18n.use(initReactI18next).init({
    resources: {
        en: {
            'common.actions': enCommonActions,
            'settings.preferences': enSettingsPreferences,
        },
        de: {
            'common.actions': deCommonActions,
            'settings.preferences': deSettingsPreferences,
        },
    },
    fallbackLng: 'en',
    interpolation: {
        escapeValue: false, // react already safes from xss
    },
});

export default i18n;
