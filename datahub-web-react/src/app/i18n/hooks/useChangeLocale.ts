import i18next from 'i18next';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { DEFAULT_LANGUAGE, LOCALE_MAP } from '@app/i18n/constants';
import { useUpdateUserLocaleSettings } from '@app/i18n/hooks/useUpdateUserLocaleSettings';
import { SupportedLanguage } from '@app/i18n/types';
import { detectBrowserLanguage, isSupportedLanguage } from '@app/i18n/utils';
import { useUserLanguage } from '@app/sharedV2/hooks/useUserLanguage';
import { setDayjsLocale } from '@utils/dayjs';

export function useChangeLocale() {
    const updateUserLocaleSettings = useUpdateUserLocaleSettings();
    const savedLanguage = useUserLanguage() ?? null;

    return useCallback(
        async (language: string | null) => {
            const effectiveLanguage: SupportedLanguage =
                language && isSupportedLanguage(language) ? language : DEFAULT_LANGUAGE;
            const localeConfig = LOCALE_MAP[effectiveLanguage];
            const previousLanguage = i18next.language;

            await i18next.loadLanguages(localeConfig.lang);
            await updateUserLocaleSettings(language);
            // Sent only once the setting is saved, so a failed save is not counted as a change.
            // Compared with the saved setting, not the shown language: explicitly picking the
            // language the browser already gave you is a real change from "no choice".
            if (language !== savedLanguage) {
                analytics.event({
                    type: EventType.LanguageChangeEvent,
                    previousLanguage,
                    newLanguage: language,
                    browserLanguage: detectBrowserLanguage() ?? null,
                });
            }
            i18next.changeLanguage(localeConfig.lang);
            await setDayjsLocale(localeConfig.dayjs);
        },
        [updateUserLocaleSettings, savedLanguage],
    );
}
