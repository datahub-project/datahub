import i18next from 'i18next';
import { useEffect } from 'react';

import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';
import dayjs from '@utils/dayjs';

export function useLanguageSync(): void {
    const localeConfig = useLocaleConfig();

    useEffect(() => {
        i18next.changeLanguage(localeConfig.lang);
        dayjs.locale(localeConfig.dayjs);
    }, [localeConfig.lang, localeConfig.dayjs]);
}
