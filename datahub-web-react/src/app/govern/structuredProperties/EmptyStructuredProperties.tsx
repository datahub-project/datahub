import { EmptyState } from '@components';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';

interface Props {
    isEmptySearch?: boolean;
}

const EmptyStructuredProperties = ({ isEmptySearch }: Props) => {
    const { t } = useTranslation('governance.structured-properties');

    return (
        <EmptyContainer>
            {isEmptySearch ? (
                <EmptyState icon={MagnifyingGlass} title={t('table.noSearchResults')} />
            ) : (
                <EmptyState icon={ListBullets} title={t('table.noPropertiesYet')} />
            )}
        </EmptyContainer>
    );
};

export default EmptyStructuredProperties;
