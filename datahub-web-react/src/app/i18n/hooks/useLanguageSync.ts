import i18next from 'i18next';
import { useEffect } from 'react';

import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';
import { setDayjsLocale } from '@utils/dayjs';

export function useLanguageSync(): void {
    const localeConfig = useLocaleConfig();

    useEffect(() => {
        i18next.changeLanguage(localeConfig.lang);
        // setDayjsLocale resolves after the locale chunk loads; ignore the promise — dayjs falls
        // back to its current locale until then, and a later language change supersedes this one.
        setDayjsLocale(localeConfig.dayjs);
    }, [localeConfig.lang, localeConfig.dayjs]);
}
