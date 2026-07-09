import { ConfigProvider } from 'antd';
import type { Locale } from 'antd/lib/locale-provider';
import React, { useEffect, useState } from 'react';

import { loadAntdLocale } from '@app/i18n/antdLocale';
import { useEffectiveLanguage } from '@app/i18n/hooks/useEffectiveLanguage';
import { useLanguageSync } from '@app/i18n/hooks/useLanguageSync';

export default function I18nProvider({ children }: { children: React.ReactNode }) {
    useLanguageSync();
    const language = useEffectiveLanguage();
    const [antdLocale, setAntdLocale] = useState<Locale>();

    useEffect(() => {
        let active = true;
        // Load antd's locale chunk on demand; until it resolves ConfigProvider uses antd's
        // built-in English default. A newer language change supersedes an in-flight load.
        loadAntdLocale(language)
            .then((locale) => {
                if (active) setAntdLocale(locale);
            })
            .catch(() => {
                // Keep the antd default on load failure rather than blocking render.
            });
        return () => {
            active = false;
        };
    }, [language]);

    return <ConfigProvider locale={antdLocale}>{children}</ConfigProvider>;
}
