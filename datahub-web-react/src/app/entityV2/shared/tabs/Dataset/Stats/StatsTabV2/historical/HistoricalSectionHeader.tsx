import { PageTitle } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

const HistoricalSectionHeader = () => {
    const { t } = useTranslation('entity.profile.stats');
    return (
        <>
            <PageTitle
                title={t('historicalSection.title')}
                subTitle={t('historicalSection.subtitle')}
                variant="sectionHeader"
            />{' '}
        </>
    );
};

export default HistoricalSectionHeader;
