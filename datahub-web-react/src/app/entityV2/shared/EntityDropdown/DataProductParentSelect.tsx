import { XCircle } from '@phosphor-icons/react/dist/csr/XCircle';
import { Empty, Select } from 'antd';
import React, { MouseEvent, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import useParentSelector from '@app/entityV2/shared/EntityDropdown/useParentSelector';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
    setSelectedParentUrn: (parent: string) => void;
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
    const theme = useTheme();
    const { t } = useTranslation('entity.shared.entityDropdown');
    const entityRegistry = useEntityRegistry();

    const {
        searchResults,
        searchQuery,
        selectedParentName,
        onSelectParent,
        handleSearch,
        clearSelectedParent,
        selectParentFromBrowser,
        setIsFocusedOnInput,
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

    const handleClear = (event: MouseEvent) => {
        event.stopPropagation();
        clearSelectedParent();
    };

    return (
        <Select
            showSearch
            allowClear
            clearIcon={<XCircle weight="fill" onClick={handleClear} />}
            filterOption={false}
            defaultActiveFirstOption={false}
            placeholder={t('dataProductSelect.placeholder')}
            value={selectedParentName}
            onSelect={onSelectParent}
            onSearch={handleSearch}
            onFocus={() => setIsFocusedOnInput(true)}
            onBlur={() => setIsFocusedOnInput(false)}
            notFoundContent={
                searchQuery ? (
                    <Empty
                        description={t('dataProductSelect.notFound')}
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        style={{ color: theme.colors.textTertiary }}
                    />
                ) : null
            }
            options={
                autoCompleteResultsLoading
                    ? []
                    : filteredResults.map((entity) => {
                          const displayName = entityRegistry.getDisplayName(entity.type, entity);
                          return {
                              value: entity.urn,
                              label: <span data-testid={`data-product-option-${displayName}`}>{displayName}</span>,
                          };
                      })
            }
            data-testid="parent-data-product-select"
        />
    );
}
