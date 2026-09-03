import { Result } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

export default function NonExistentEntityPage() {
    const { t } = useTranslation('entity.shared.emptyStates');
    return <Result status="404" title={t('notFound.title')} subTitle={t('notFound.subtitle')} />;
}
