import React from 'react';
import { useTranslation } from 'react-i18next';

export function AIChat() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    return <div>{t('multiStep.chat.placeholder')}</div>;
}
