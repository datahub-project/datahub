import { Result } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

export default function NonExistentEntityPage() {
    const { t } = useTranslation('shared.misc');
    return <Result status="404" title={t('noEntityFound.title')} subTitle={t('noEntityFound.subtitle')} />;
}
