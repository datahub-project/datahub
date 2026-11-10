import { EmptyState } from '@components';
import { Tray } from '@phosphor-icons/react/dist/csr/Tray';
import React from 'react';
import { useTranslation } from 'react-i18next';

export const EmptyAnnouncements = () => {
    const { t } = useTranslation('home.v2');
    return <EmptyState icon={Tray} title={t('announcements.empty')} />;
};
