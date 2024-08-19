import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';


import * as enEmptyMessage from './locales/en/empty-message.json';
import * as enForm from './locales/en/form.json';
import * as enReactCron from './locales/en/react-cron.json';
import * as enTheme from './locales/en/theme.json';
import * as enTranslation from './locales/en/translation.json';

import * as ptBREmptyMessage from './locales/pt-br/empty-message.json';
import * as ptBRForm from './locales/pt-br/form.json';
import * as ptBRReactCron from './locales/pt-br/react-cron.json';
import * as ptBRTheme from './locales/pt-br/theme.json';
import * as ptBRTranslation from './locales/pt-br/translation.json';

const resources = {
    en: {
        translation: enTranslation.default,
        'empty-message': enEmptyMessage.default,
        form: enForm.default,
        'react-cron': enReactCron.default,
        theme: enTheme.default,
    },
    pt_br: {
        translation: ptBRTranslation.default,
        'empty-message': ptBREmptyMessage.default,
        form: ptBRForm.default,
        'react-cron': ptBRReactCron.default,
        theme: ptBRTheme.default,
    },
};

i18n
    .use(initReactI18next)
    .init({
        fallbackLng: 'en',
        interpolation: {
            escapeValue: false,
        },
        resources
    });

export default i18n;
