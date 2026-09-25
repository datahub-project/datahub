import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/homeV3/styledComponents';

const EmptySection = () => {
    const { t } = useTranslation('modules');
    return (
        <EmptyContainer>
            <Text>{t('assetCollection.noAssetsFound')}</Text>
        </EmptyContainer>
    );
};

export default EmptySection;
