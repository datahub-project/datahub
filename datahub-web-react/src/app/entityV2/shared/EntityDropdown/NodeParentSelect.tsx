import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { filterResultsForMove } from '@app/entityV2/shared/EntityDropdown/nodeParentSelectUtils';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { deriveGlossaryLabelFromUrn } from '@app/glossaryV2/utils';
import { useGlossaryTreeEntities } from '@app/shared/tags/useGlossaryTreeEntities';
import { TermTreeOption, useTermTreeOptions } from '@app/shared/tags/useTermTreeOptions';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader, SimpleSelect } from '@src/alchemy-components';

import { useGetAutoCompleteResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, GlossaryNode } from '@types';

// Caret column (20px) + caret→content gap (8px). Matches AddTermsModal so this picker reads
// visually like the term picker the user is already familiar with.
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

const NodeLabel = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 220px;
    font-size: 13px;
    color: ${(props) => props.theme.colors.text};
`;

const SelectedValueRow = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
`;

interface Props {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
    isMoving?: boolean;
    /** Optional companion callback fired alongside `setSelectedParentUrn` with the resolved
     * parent entity (or `null` when the user clears the selection). Lets callers read the
     * parent's `displayProperties.colorHex` etc. without having to refetch the entity. */
    onSelectParent?: (parent: GlossaryNode | null) => void;
}

const AUTOCOMPLETE_LIMIT = 10;

/**
 * Picker for a single glossary node — used by the create/edit/move glossary modals.
 *
 * Reuses the same browse-and-search data flow as `AddTermsModal` (lazy-loaded tree from
 * `useGlossaryTreeEntities` + autocomplete search), but filters the stream to glossary *nodes*
 * since terms can't be parents. Nodes here are selectable rows (unlike AddTermsModal where they
 * act as non-selectable headers above term leaves), so we override the disabled set to only
 * include synthetic loading placeholders.
 */
function NodeParentSelect({ selectedParentUrn, setSelectedParentUrn, isMoving, onSelectParent }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    const { urn: entityDataUrn, entityType } = useEntityData();
    const shouldHideSelf = isMoving && entityType === EntityType.GlossaryNode && !!entityDataUrn;

    const [searchText, setSearchText] = useState('');
    const [getAutoComplete, { data: searchData, loading: searchLoading }] = useGetAutoCompleteResultsLazyQuery();

    const handleSearch = useCallback(
        (text: string) => {
            const trimmed = text.trim();
            setSearchText(trimmed);
            if (trimmed) {
                getAutoComplete({
                    variables: { input: { type: EntityType.GlossaryNode, query: trimmed, limit: AUTOCOMPLETE_LIMIT } },
                });
            }
        },
        [getAutoComplete],
    );

    const {
        entities: treeEntities,
        entityCache: treeEntityCache,
        expandedNodes,
        fetchingNodes,
        expandNode,
        collapseNode,
        isLoading: treeLoading,
    } = useGlossaryTreeEntities();

    // useGlossaryTreeEntities returns nodes + root terms together (it's the same data flow the
    // sidebar uses). For a parent picker we only want nodes — strip terms here so they never
    // make it into `useTermTreeOptions`.
    const nodeOnlyTreeEntities = useMemo<Entity[]>(
        () => treeEntities.filter((e) => e.type === EntityType.GlossaryNode),
        [treeEntities],
    );

    const searchEntities = useMemo<Entity[]>(
        () =>
            ((searchData?.autoComplete?.entities ?? []) as Entity[]).filter((e) => e.type === EntityType.GlossaryNode),
        [searchData],
    );

    const isSearching = searchText.length > 0;
    const rawSource = isSearching ? searchEntities : nodeOnlyTreeEntities;

    // Move modal: drop the entity itself and any descendant — you can't move a node under
    // itself or any of its own children.
    const sourceEntities = useMemo<Entity[]>(() => {
        if (!shouldHideSelf) return rawSource;
        return rawSource.filter((e) => filterResultsForMove(e as GlossaryNode, entityDataUrn));
    }, [rawSource, shouldHideSelf, entityDataUrn]);

    const { visibleOptions, allOptions, nodesWithChildren } = useTermTreeOptions({
        entities: sourceEntities,
        expandedNodes: isSearching ? undefined : expandedNodes,
        loadingNodeUrns: isSearching ? undefined : fetchingNodes,
    });

    // Nodes are selectable in this picker (the whole point — pick a parent). The shared
    // `useTermTreeOptions` defaults to disabling node rows because AddTermsModal treats them as
    // headers; override here so only the synthetic loading placeholders are disabled.
    const disabledValues = useMemo(
        () => allOptions.filter((o) => o.isLoadingPlaceholder).map((o) => o.value),
        [allOptions],
    );

    // Ensure the currently-selected URN always has a matching option so the alchemy chip can
    // render a label even when the tree branch is collapsed or the user has typed a search query
    // that doesn't include it. Falls back to the entity cache, then to a URN-derived label.
    const combinedOptions = useMemo<TermTreeOption[]>(() => {
        if (!selectedParentUrn) return allOptions;
        if (allOptions.some((o) => o.value === selectedParentUrn)) return allOptions;
        const cached = treeEntityCache[selectedParentUrn] as GlossaryNode | undefined;
        const rawLabel = cached
            ? entityRegistry.getDisplayName(cached.type, cached)
            : deriveGlossaryLabelFromUrn(selectedParentUrn);
        // `getDisplayName` can fall back to the URN; guard so chips never read `urn:li:…`.
        const label =
            rawLabel && !rawLabel.startsWith('urn:') ? rawLabel : deriveGlossaryLabelFromUrn(selectedParentUrn);
        return [
            ...allOptions,
            {
                value: selectedParentUrn,
                label,
                entity: cached,
                color: cached?.displayProperties?.colorHex || generateColor(selectedParentUrn),
            },
        ];
    }, [allOptions, selectedParentUrn, treeEntityCache, entityRegistry, generateColor]);

    const handleCaretClick = useCallback(
        (nodeUrn: string) => {
            if (expandedNodes.has(nodeUrn)) collapseNode(nodeUrn);
            else expandNode(nodeUrn);
        },
        [expandedNodes, collapseNode, expandNode],
    );

    const renderOption = useCallback(
        (option: TermTreeOption) => {
            const depth = option.depth || 0;
            if (option.isLoadingPlaceholder) {
                return (
                    <LoadingRow $depth={depth}>
                        <Loader size="xs" justifyContent="flex-start" alignItems="center" />
                    </LoadingRow>
                );
            }
            // Every option here represents a node (terms were filtered upstream). Show the caret
            // when either (a) the search path already surfaced descendants, (b) the browse path
            // hasn't expanded the node yet and it might have children (`isEmptyNode`).
            const hasChildren = nodesWithChildren.has(option.value) || option.isEmptyNode;
            const isExpanded = expandedNodes.has(option.value);
            const CaretIcon = isExpanded ? CaretDown : CaretRight;
            const color = option.color ?? generateColor(option.value);

            return (
                <OptionRow $depth={depth} data-testid={`node-parent-option-${option.label}`}>
                    <CaretButton
                        type="button"
                        aria-label={isExpanded ? `Collapse ${option.label}` : `Expand ${option.label}`}
                        aria-expanded={isExpanded}
                        // Stops alchemy SimpleSelect from treating this as an option click.
                        onMouseDown={(e) => e.preventDefault()}
                        onClick={(e) => {
                            e.stopPropagation();
                            if (hasChildren) handleCaretClick(option.value);
                        }}
                        style={{ visibility: hasChildren ? 'visible' : 'hidden' }}
                    >
                        <CaretIcon size={14} weight="regular" />
                    </CaretButton>
                    <GlossaryColoredIcon color={color} icon={BookmarksSimple} size={20} iconSize={12} />
                    <NodeLabel>{option.label}</NodeLabel>
                </OptionRow>
            );
        },
        [nodesWithChildren, expandedNodes, generateColor, handleCaretClick],
    );

    const renderSelectedValue = useCallback(
        (option: TermTreeOption) => (
            <SelectedValueRow>
                <GlossaryColoredIcon
                    color={option.color ?? generateColor(option.value)}
                    icon={BookmarksSimple}
                    size={18}
                    iconSize={10}
                />
                <NodeLabel>{option.label}</NodeLabel>
            </SelectedValueRow>
        ),
        [generateColor],
    );

    const onUpdate = useCallback(
        (urns: string[]) => {
            // SimpleSelect always emits an array; for single-select the first entry is the
            // current value and an empty array means "cleared".
            const newUrn = urns[0] || '';
            setSelectedParentUrn(newUrn);
            if (onSelectParent) {
                // Resolve the entity from whichever bucket has it: the tree cache covers the
                // browse path, the option list covers the search path.
                const fromOption = combinedOptions.find((o) => o.value === newUrn)?.entity as GlossaryNode | undefined;
                const entity = newUrn ? fromOption || (treeEntityCache[newUrn] as GlossaryNode | undefined) : null;
                onSelectParent(entity ?? null);
            }
        },
        [setSelectedParentUrn, onSelectParent, combinedOptions, treeEntityCache],
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
            placeholder={t('createGlossary.searchParentPlaceholder')}
            width="full"
            dataTestId="node-parent-select"
        />
    );
}

export default NodeParentSelect;
