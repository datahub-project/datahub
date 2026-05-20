import i18next from 'i18next';
import { useCallback } from 'react';

import { DEFAULT_LANGUAGE, LOCALE_MAP } from '@app/i18n/constants';
import { useUpdateUserLocaleSettings } from '@app/i18n/hooks/useUpdateUserLocaleSettings';
import { SupportedLanguage } from '@app/i18n/types';
import { isSupportedLanguage } from '@app/i18n/utils';
import dayjs from '@utils/dayjs';

export function useChangeLocale() {
    const updateUserLocaleSettings = useUpdateUserLocaleSettings();

    return useCallback(
        async (language: string | null) => {
            const effectiveLanguage: SupportedLanguage =
                language && isSupportedLanguage(language) ? language : DEFAULT_LANGUAGE;
            const localeConfig = LOCALE_MAP[effectiveLanguage];

            await i18next.loadLanguages(localeConfig.lang);
            await updateUserLocaleSettings(language);
            i18next.changeLanguage(localeConfig.lang);
            dayjs.locale(localeConfig.dayjs);
        },
        [updateUserLocaleSettings],
    );
}
