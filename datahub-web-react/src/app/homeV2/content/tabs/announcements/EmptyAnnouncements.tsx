import { Empty } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

export const EmptyAnnouncements = () => {
    const { t } = useTranslation('home.v2');
    return <Empty description={t('announcements.empty')} />;
};
