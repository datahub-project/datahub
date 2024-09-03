import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import * as enTranslation from './locales/en/translation.json';
import * as ptBRTranslation from './locales/pt-br/translation.json';

const resources = {
    en: {
        translation: enTranslation.default,
    },
    pt_br: {
        translation: ptBRTranslation.default,
    },
};

i18n.use(initReactI18next).init({
    fallbackLng: 'en',
    interpolation: {
        escapeValue: false,
    },
    resources,
});

export default i18n;
