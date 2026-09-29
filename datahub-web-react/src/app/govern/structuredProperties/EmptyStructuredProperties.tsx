import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

interface Props {
    isEmptySearch?: boolean;
}

const EmptyStructuredProperties = ({ isEmptySearch }: Props) => {
    const { t } = useTranslation('governance.structured-properties');

    return (
        <EmptyContainer>
            {isEmptySearch ? (
                <Text size="lg" color="gray" weight="bold">
                    {t('table.noSearchResults')}
                </Text>
            ) : (
                <>
                    <EmptyFormsImage />
                    <Text size="md" color="gray" weight="bold">
                        {t('table.noPropertiesYet')}
                    </Text>
                </>
            )}
        </EmptyContainer>
    );
};

export default EmptyStructuredProperties;
