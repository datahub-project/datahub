import { useMemo } from 'react';

import { useDefaultLanguage } from '@app/i18n/hooks/useDefaultLanguage';
import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { SupportedLanguage } from '@app/i18n/types';
import { detectBrowserLanguage, isSupportedLanguage } from '@app/i18n/utils';
import { useUserLanguage } from '@app/shared/hooks/useUserLanguage';

export function useEffectiveLanguage(): SupportedLanguage {
    const i18nEnabled = useIsI18nEnabled();
    const userLanguage = useUserLanguage();
    const defaultLanguage = useDefaultLanguage();

    return useMemo(() => {
        // i18n must be enabled for any non-English locale to take effect.
        if (!i18nEnabled) return defaultLanguage;
        // An explicit in-app choice (Settings -> Preferences) always wins.
        if (userLanguage && isSupportedLanguage(userLanguage)) return userLanguage;
        // Otherwise honor the user's browser/OS language, falling back to the default.
        return detectBrowserLanguage() ?? defaultLanguage;
    }, [i18nEnabled, userLanguage, defaultLanguage]);
}
