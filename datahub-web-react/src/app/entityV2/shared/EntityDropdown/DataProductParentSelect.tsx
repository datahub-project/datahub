import React, { useCallback, useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { filterResultsForMove } from '@app/entityV2/shared/EntityDropdown/dataProductParentSelectUtils';
import useParentSelector from '@app/entityV2/shared/EntityDropdown/useParentSelector';
import { DataProductLink } from '@app/sharedV2/tags/DataProductLink';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { SelectOption, SimpleSelect, Text } from '@src/alchemy-components';

import { DataProduct, EntityType } from '@types';

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

    const entitiesByUrn = useMemo(() => {
        const map = new Map<string, DataProduct>();
        filteredResults.forEach((entity) => {
            if (entity.type === EntityType.DataProduct) {
                map.set(entity.urn, entity as DataProduct);
            }
        });
        return map;
    }, [filteredResults]);

    const renderDataProductOption = useCallback(
        (option: SelectOption) => {
            const dataProduct = entitiesByUrn.get(option.value);
            if (!dataProduct) {
                return option.label;
            }

            return <DataProductLink dataProduct={dataProduct} readOnly fontSize={14} />;
        },
        [entitiesByUrn],
    );

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
            renderCustomOptionText={renderDataProductOption}
            renderCustomSelectedValue={renderDataProductOption}
            selectLabelProps={{ variant: 'custom' }}
        />
    );
}
