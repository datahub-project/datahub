import { useMemo } from 'react';

import { useDefaultLanguage } from '@app/i18n/hooks/useDefaultLanguage';
import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { SupportedLanguage } from '@app/i18n/types';
import { resolveEffectiveLanguage } from '@app/i18n/utils';
import { useUserLanguage } from '@app/sharedV2/hooks/useUserLanguage';

export function useEffectiveLanguage(): SupportedLanguage {
    const i18nEnabled = useIsI18nEnabled();
    const userLanguage = useUserLanguage();
    const defaultLanguage = useDefaultLanguage();

    return useMemo(
        () => resolveEffectiveLanguage(i18nEnabled, userLanguage, defaultLanguage),
        [i18nEnabled, userLanguage, defaultLanguage],
    );
}
