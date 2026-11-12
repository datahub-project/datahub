import { Result } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

export const UnauthorizedPage = () => {
    const { t } = useTranslation('misc');
    return (
        <>
            <Result
                status="403"
                title={t('authorization.unauthorizedTitle')}
                subTitle={t('authorization.unauthorizedSubtitle')}
            />
        </>
    );
};
