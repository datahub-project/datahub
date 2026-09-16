import { Select } from '@components';
import React, { useCallback, useState } from 'react';

import { LANGUAGE_OPTIONS } from '@app/i18n/constants';
import { useChangeLocale } from '@app/i18n/hooks/useChangeLocale';
import { useEffectiveLanguage } from '@app/i18n/hooks/useEffectiveLanguage';
import { SupportedLanguage } from '@app/i18n/types';

export function LanguageSelect() {
    const effectiveLanguage = useEffectiveLanguage();
    const changeLocale = useChangeLocale();
    const [selectedLanguage, setSelectedLanguage] = useState<SupportedLanguage>(effectiveLanguage);

    const handleUpdate = useCallback(
        async ([language]: string[]) => {
            if (language) {
                setSelectedLanguage(language as SupportedLanguage);
                await changeLocale(language);
            }
        },
        [changeLocale],
    );

    return <Select options={LANGUAGE_OPTIONS} values={[selectedLanguage]} onUpdate={handleUpdate} />;
}
