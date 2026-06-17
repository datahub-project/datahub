import { LOCALE_MAP } from '@app/i18n/constants';
import { SupportedLanguage } from '@app/i18n/types';

export function isSupportedLanguage(lang: string): lang is SupportedLanguage {
    return lang in LOCALE_MAP;
}
