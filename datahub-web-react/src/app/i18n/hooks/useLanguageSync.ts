import i18next from 'i18next';
import { useEffect } from 'react';

import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';
import { hasLocaleBundleFailed } from '@src/i18n/localeBackend';
import { setDayjsLocale } from '@utils/dayjs';

export function useLanguageSync(): void {
    const localeConfig = useLocaleConfig();

    useEffect(() => {
        // Re-request the language when its bundle failed, even if it is already selected —
        // otherwise a failed load has no way back short of a page reload.
        if (i18next.language !== localeConfig.lang || hasLocaleBundleFailed(localeConfig.lang)) {
            i18next.changeLanguage(localeConfig.lang);
        }
        // setDayjsLocale resolves after the locale chunk loads; ignore the promise — dayjs falls
        // back to its current locale until then, and a later language change supersedes this one.
        // eslint-disable-next-line no-void
        void setDayjsLocale(localeConfig.dayjs);
    }, [localeConfig.lang, localeConfig.dayjs]);
}
