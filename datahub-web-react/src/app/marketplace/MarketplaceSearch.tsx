import { SearchBar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

type Props = {
    value: string;
    onChange: (value: string) => void;
};

export default function MarketplaceSearch({ value, onChange }: Props) {
    const { t } = useTranslation('misc');

    return (
        <SearchBar
            placeholder={t('marketplace.searchPlaceholder')}
            value={value}
            onChange={onChange}
            data-testid="marketplace-sidebar-search-input"
        />
    );
}
