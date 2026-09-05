import { SearchBar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

type Props = {
    value: string;
    onChange: (value: string) => void;
};

export default function DataProductsSearch({ value, onChange }: Props) {
    const { t } = useTranslation('misc');

    return (
        <SearchBar
            placeholder={t('dataProducts.searchPlaceholder')}
            value={value}
            onChange={onChange}
            data-testid="data-products-sidebar-search-input"
        />
    );
}
