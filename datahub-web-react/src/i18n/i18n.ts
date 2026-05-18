import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import deCommonActions from '@src/i18n/locales/de/common.actions.json';
import enCommonActions from '@src/i18n/locales/en/common.actions.json';

i18n.use(initReactI18next).init({
    resources: {
        en: {
            'common.actions': enCommonActions,
        },
        de: {
            'common.actions': deCommonActions,
        },
    },
    fallbackLng: 'en',
    interpolation: {
        escapeValue: false, // react already safes from xss
    },
});

export default i18n;
