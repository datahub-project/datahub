import i18n from 'i18next';
import languageDetector from 'i18next-browser-languagedetector';
import { initReactI18next } from 'react-i18next';
import translation_en from './en.json';
import translation_zh from './zh.json';

const resources = {
    en: {
        translation: translation_en,
    },
    zh: {
        translation: translation_zh,
    },
};

i18n.use(languageDetector)
    .use(initReactI18next).init({
    resources,
    // use browser language settings, fall back language is en
    lng: navigator.language,
    fallbackLng:"en",
    interpolation: {
        escapeValue: false,
    },
});
export default i18n;