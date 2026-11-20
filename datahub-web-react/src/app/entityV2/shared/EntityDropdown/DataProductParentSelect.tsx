import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { filterResultsForMove } from '@app/entityV2/shared/EntityDropdown/dataProductParentSelectUtils';
import { useDataProductTreeEntities } from '@app/entityV2/shared/EntityDropdown/useDataProductTreeEntities';
import {
    DataProductTreeOption,
    useDataProductTreeOptions,
} from '@app/entityV2/shared/EntityDropdown/useDataProductTreeOptions';
import { DataProductLink } from '@app/sharedV2/tags/DataProductLink';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader, SimpleSelect } from '@src/alchemy-components';

import { useScrollDataProductsLazyQuery } from '@graphql/marketplaceBrowse.generated';
import { DataProduct, EntityType } from '@types';

// Caret column (20px) + caret→content gap (8px). Matches NodeParentSelect / AddTermsModal.
const CARET_COLUMN_WIDTH = 28;

const OptionRow = styled.div<{ $depth: number }>`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    padding-left: ${(props) => props.$depth * 16}px;
`;

const LoadingRow = styled(OptionRow)`
    flex: 1;
    padding-left: ${(props) => props.$depth * 16 + CARET_COLUMN_WIDTH}px;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    // Hide alchemy SimpleSelect's adjacent checkbox/tick — loading rows are static.
    & ~ * {
        display: none !important;
    }
`;

const CaretButton = styled.button`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    background: transparent;
    border: none;
    color: ${(props) => props.theme.colors.icon};
    cursor: pointer;
    flex-shrink: 0;

    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

const OptionLabel = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 280px;
    font-size: 13px;
    color: ${(props) => props.theme.colors.text};
`;

type Props = {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string, name?: string) => void;
    /** When editing or moving, exclude this data product. */
    excludeUrn?: string;
    /** Display name for a pre-selected parent (e.g. edit modal). */
    initialParentName?: string;
};

const SEARCH_COUNT = 25;

/**
 * Parent data-product picker — glossary-style browse tree with expand loading + flat search.
 */
export default function DataProductParentSelect({
    selectedParentUrn,
    setSelectedParentUrn,
    excludeUrn,
    initialParentName,
}: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const entityRegistry = useEntityRegistry();

    const [searchText, setSearchText] = useState('');
    const [scrollSearch, { data: searchData, loading: searchLoading }] = useScrollDataProductsLazyQuery();

    const {
        entities: treeEntities,
        entityCache: treeEntityCache,
        expandedNodes,
        fetchingNodes,
        expandNode,
        collapseNode,
        isLoading: treeLoading,
    } = useDataProductTreeEntities();

    const handleSearch = useCallback(
        (text: string) => {
            const trimmed = text.trim();
            setSearchText(trimmed);
            if (trimmed) {
                scrollSearch({
                    variables: {
                        input: {
                            scrollId: null,
                            query: trimmed,
                            types: [EntityType.DataProduct],
                            count: SEARCH_COUNT,
                            searchFlags: { skipCache: true },
                        },
                    },
                });
            }
        },
        [scrollSearch],
    );

    const searchEntities = useMemo<DataProduct[]>(() => {
        return (
            searchData?.scrollAcrossEntities?.searchResults
                ?.map((r) => r.entity)
                .filter((e): e is DataProduct => e?.__typename === 'DataProduct') ?? []
        );
    }, [searchData]);

    const isSearching = searchText.length > 0;
    const rawSource = isSearching ? searchEntities : treeEntities;

    const sourceEntities = useMemo<DataProduct[]>(() => {
        if (!excludeUrn) return rawSource;
        return rawSource.filter((e) => filterResultsForMove(e, excludeUrn));
    }, [rawSource, excludeUrn]);

    const { visibleOptions, allOptions, nodesWithChildren } = useDataProductTreeOptions({
        entities: sourceEntities,
        expandedNodes: isSearching ? undefined : expandedNodes,
        loadingNodeUrns: isSearching ? undefined : fetchingNodes,
    });

    const disabledValues = useMemo(
        () => allOptions.filter((o) => o.isLoadingPlaceholder).map((o) => o.value),
        [allOptions],
    );

    const combinedOptions = useMemo<DataProductTreeOption[]>(() => {
        if (!selectedParentUrn) return allOptions;
        if (allOptions.some((o) => o.value === selectedParentUrn)) return allOptions;
        const cached = treeEntityCache[selectedParentUrn];
        const label =
            (cached ? entityRegistry.getDisplayName(cached.type, cached) : undefined) ||
            initialParentName ||
            selectedParentUrn.split(':').pop() ||
            selectedParentUrn;
        return [
            ...allOptions,
            {
                value: selectedParentUrn,
                label,
                entity: cached,
            },
        ];
    }, [allOptions, selectedParentUrn, treeEntityCache, entityRegistry, initialParentName]);

    const handleCaretClick = useCallback(
        (nodeUrn: string) => {
            if (expandedNodes.has(nodeUrn)) collapseNode(nodeUrn);
            else expandNode(nodeUrn);
        },
        [expandedNodes, collapseNode, expandNode],
    );

    const renderOption = useCallback(
        (option: DataProductTreeOption) => {
            const depth = option.depth || 0;
            if (option.isLoadingPlaceholder) {
                return (
                    <LoadingRow $depth={depth}>
                        <Loader size="xs" justifyContent="flex-start" alignItems="center" />
                    </LoadingRow>
                );
            }

            const hasChildren = nodesWithChildren.has(option.value) || !!option.isEmptyNode;
            const isExpanded = expandedNodes.has(option.value);
            const CaretIcon = isExpanded ? CaretDown : CaretRight;

            return (
                <OptionRow $depth={depth} data-testid={`parent-data-product-option-${option.value}`}>
                    <CaretButton
                        type="button"
                        aria-label={isExpanded ? `Collapse ${option.label}` : `Expand ${option.label}`}
                        aria-expanded={isExpanded}
                        onMouseDown={(e) => e.preventDefault()}
                        onClick={(e) => {
                            e.stopPropagation();
                            if (hasChildren) handleCaretClick(option.value);
                        }}
                        style={{ visibility: hasChildren ? 'visible' : 'hidden' }}
                    >
                        <CaretIcon size={14} weight="regular" />
                    </CaretButton>
                    {option.entity ? (
                        <DataProductLink dataProduct={option.entity} readOnly fontSize={13} />
                    ) : (
                        <OptionLabel>{option.label}</OptionLabel>
                    )}
                </OptionRow>
            );
        },
        [nodesWithChildren, expandedNodes, handleCaretClick],
    );

    const renderSelectedValue = useCallback((option: DataProductTreeOption) => {
        if (option.entity) {
            return <DataProductLink dataProduct={option.entity} readOnly fontSize={14} />;
        }
        return <OptionLabel>{option.label}</OptionLabel>;
    }, []);

    const onUpdate = useCallback(
        (urns: string[]) => {
            const newUrn = urns[0] || '';
            if (!newUrn) {
                setSelectedParentUrn('');
                return;
            }
            const fromOption = combinedOptions.find((o) => o.value === newUrn);
            const entity = fromOption?.entity || treeEntityCache[newUrn];
            const displayName = entity
                ? entityRegistry.getDisplayName(entity.type, entity)
                : fromOption?.label || newUrn;
            setSelectedParentUrn(newUrn, displayName);
        },
        [setSelectedParentUrn, combinedOptions, treeEntityCache, entityRegistry],
    );

    const values = useMemo(() => (selectedParentUrn ? [selectedParentUrn] : []), [selectedParentUrn]);

    return (
        <SimpleSelect
            showSearch
            showClear
            onSearchChange={handleSearch}
            values={values}
            onUpdate={onUpdate}
            options={visibleOptions}
            disabledValues={disabledValues}
            combinedSelectedAndSearchOptions={combinedOptions}
            renderCustomOptionText={renderOption}
            renderCustomSelectedValue={renderSelectedValue}
            selectLabelProps={{ variant: 'custom' }}
            filterResultsByQuery={false}
            isLoading={isSearching ? searchLoading : treeLoading}
            placeholder={t('dataProductSelect.placeholder')}
            width="full"
            dataTestId="parent-data-product-select"
            emptyState={<OptionLabel>{t('dataProductSelect.notFound')}</OptionLabel>}
        />
    );
}
