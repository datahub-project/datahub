import { ConfigProvider } from 'antd';
import React from 'react';

import { useLanguageSync } from '@app/i18n/hooks/useLanguageSync';
import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';

export default function I18nProvider({ children }: { children: React.ReactNode }) {
    useLanguageSync();
    const { antd } = useLocaleConfig();
    return <ConfigProvider locale={antd}>{children}</ConfigProvider>;
}
