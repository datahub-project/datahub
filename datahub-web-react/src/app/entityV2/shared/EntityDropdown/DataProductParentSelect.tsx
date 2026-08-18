import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';

import useParentSelector from '@app/entityV2/shared/EntityDropdown/useParentSelector';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { SelectOption, SimpleSelect, Text } from '@src/alchemy-components';

import { DataProduct, EntityType } from '@types';

/** Exclude self and any result that already lists self as an ancestor. */
export function filterResultsForMove(entity: DataProduct, entityUrn: string) {
    return (
        entity.urn !== entityUrn &&
        entity.type === EntityType.DataProduct &&
        !entity.parentDataProducts?.some((ancestor) => ancestor.urn === entityUrn)
    );
}

type Props = {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string, name?: string) => void;
    /** When editing or moving, exclude this data product. */
    excludeUrn?: string;
    /** Display name for a pre-selected parent (e.g. edit modal). */
    initialParentName?: string;
};

export default function DataProductParentSelect({
    selectedParentUrn,
    setSelectedParentUrn,
    excludeUrn,
    initialParentName,
}: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const entityRegistry = useEntityRegistry();

    const {
        searchResults,
        selectedParentName,
        handleSearch,
        clearSelectedParent,
        selectParentFromBrowser,
        autoCompleteResultsLoading,
    } = useParentSelector({
        entityType: EntityType.DataProduct,
        entityData: null,
        selectedParentUrn,
        setSelectedParentUrn,
    });

    useEffect(() => {
        if (selectedParentUrn && initialParentName) {
            selectParentFromBrowser(selectedParentUrn, initialParentName);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [initialParentName]);

    const filteredResults = excludeUrn
        ? searchResults.filter((r) => filterResultsForMove(r as DataProduct, excludeUrn))
        : searchResults;

    const searchOptions: SelectOption[] = filteredResults.map((entity) => ({
        value: entity.urn,
        label: entityRegistry.getDisplayName(entity.type, entity),
    }));

    const combinedByValue = new Map<string, SelectOption>();
    if (selectedParentUrn) {
        const fromSearch = searchOptions.find((option) => option.value === selectedParentUrn);
        combinedByValue.set(selectedParentUrn, {
            value: selectedParentUrn,
            label: fromSearch?.label || selectedParentName || selectedParentUrn,
        });
    }
    searchOptions.forEach((option) => combinedByValue.set(option.value, option));
    const combinedOptions = Array.from(combinedByValue.values());

    const values = selectedParentUrn ? [selectedParentUrn] : [];

    const handleUpdate = (urns: string[]) => {
        const newUrn = urns[0] || '';
        if (!newUrn) {
            clearSelectedParent();
            return;
        }
        const fromSearch = searchResults.find((result) => result.urn === newUrn);
        const displayName = fromSearch
            ? entityRegistry.getDisplayName(fromSearch.type, fromSearch)
            : selectedParentName || newUrn;
        selectParentFromBrowser(newUrn, displayName);
        setSelectedParentUrn(newUrn, displayName);
    };

    return (
        <SimpleSelect
            showSearch
            showClear
            onSearchChange={handleSearch}
            values={values}
            onUpdate={handleUpdate}
            onClear={clearSelectedParent}
            options={searchOptions}
            combinedSelectedAndSearchOptions={combinedOptions}
            filterResultsByQuery={false}
            isLoading={autoCompleteResultsLoading}
            placeholder={t('dataProductSelect.placeholder')}
            width="full"
            dataTestId="parent-data-product-select"
            emptyState={<Text size="sm">{t('dataProductSelect.notFound')}</Text>}
        />
    );
}
