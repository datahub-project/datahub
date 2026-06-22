import { LOCALE_MAP } from '@app/i18n/constants';
import { useEffectiveLanguage } from '@app/i18n/hooks/useEffectiveLanguage';
import { LocaleConfig } from '@app/i18n/types';

export function useLocaleConfig(): LocaleConfig {
    const effectiveLanguage = useEffectiveLanguage();
    return LOCALE_MAP[effectiveLanguage];
}
