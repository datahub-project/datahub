import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { SupportedLanguage } from '@app/i18n/types';

export function useDefaultLanguage(): SupportedLanguage {
    return DEFAULT_LANGUAGE;
}
