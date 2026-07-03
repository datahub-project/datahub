import { useMemo } from 'react';

import { useDefaultLanguage } from '@app/i18n/hooks/useDefaultLanguage';
import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { SupportedLanguage } from '@app/i18n/types';
import { isSupportedLanguage } from '@app/i18n/utils';
import { useUserLanguage } from '@app/shared/hooks/useUserLanguage';

export function useEffectiveLanguage(): SupportedLanguage {
    const i18nEnabled = useIsI18nEnabled();
    const userLanguage = useUserLanguage();
    const defaultLanguage = useDefaultLanguage();

    return useMemo(
        () => (i18nEnabled && userLanguage && isSupportedLanguage(userLanguage) ? userLanguage : defaultLanguage),
        [i18nEnabled, userLanguage, defaultLanguage],
    );
}
