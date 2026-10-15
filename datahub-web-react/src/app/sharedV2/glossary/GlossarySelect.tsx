import { Loader, SimpleSelect } from '@components';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { deriveGlossaryLabelFromUrn } from '@app/glossaryV2/utils';
import { TagTermLabel } from '@app/shared/tags/TagTermLabel';
import { useEntityPickerState } from '@app/shared/tags/useEntityPickerState';
import { useGlossaryTreeEntities } from '@app/shared/tags/useGlossaryTreeEntities';
import { TermTreeOption, useTermTreeOptions } from '@app/shared/tags/useTermTreeOptions';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity, EntityType, GlossaryTerm } from '@types';

const CARET_COLUMN_WIDTH = 28;

const OptionRow = styled.div<{ $depth: number; $offsetForCaret?: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    padding-left: ${(props) => props.$depth * 16 + (props.$offsetForCaret ? CARET_COLUMN_WIDTH : 0)}px;
`;

const NonSelectableRow = styled(OptionRow)`
    flex: 1;
    pointer-events: none;
    & ~ * {
        display: none !important;
    }
`;

const LoadingRow = styled(NonSelectableRow)`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
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
    pointer-events: auto;

    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

type Props = {
    selectedUrns: string[];
    onUpdate: (urns: string[]) => void;
    placeholder?: string;
    width?: number | 'full' | 'fit-content';
    showSearch?: boolean;
    existingUrns?: string[];
    dataTestId?: string;
    /** When true, glossary nodes (parents) are selectable; when false, they're expand/collapse headers only. */
    areNodesSelectable?: boolean;
    /**
     * Pre-resolved entities for already-selected urns (e.g. from an existing filter being
     * edited). Seeds the entity cache so pre-selected terms render real display names
     * instead of urn-derived fallbacks.
     */
    defaultValues?: { urn: string; entity?: Entity | null }[];
};

/**
 * Reusable glossary select component with tree browsing and search.
 * Supports selecting both glossary terms and nodes (parents and children).
 * Used in both AddTermsModal (sidebar) and GlossarySelector (policy form).
 */
export default function GlossarySelect({
    selectedUrns,
    onUpdate,
    placeholder = 'Search for glossary terms or nodes...',
    width = 'full',
    showSearch = true,
    existingUrns = [],
    dataTestId,
    areNodesSelectable = false,
    defaultValues = [],
}: Props) {
    const { t } = useTranslation('shared.tags');
    const entityRegistry = useEntityRegistryV2();
    const generateColor = useGenerateGlossaryColorFromPalette();

    // Picker state owns the autocomplete search path.
    const {
        entityCache,
        searchText,
        handleSearch,
        currentEntities: searchEntities,
        isLoading: searchLoading,
    } = useEntityPickerState({ entityType: EntityType.GlossaryTerm, defaultValues });

    // Glossary-tree data owns the browse path (roots + lazy-loaded children).
    const {
        entities: treeEntities,
        entityCache: treeEntityCache,
        expandedNodes,
        fetchingNodes,
        expandNode,
        collapseNode,
        isLoading: treeLoading,
    } = useGlossaryTreeEntities();

    // Browse when the search input is empty; flatten autocomplete results when the user is searching.
    const isSearching = searchText.length > 0;
    const sourceEntities = isSearching ? searchEntities : treeEntities;

    // Merged cache from both paths
    const mergedEntityCache = useMemo<Record<string, Entity>>(
        () => ({ ...treeEntityCache, ...entityCache }),
        [treeEntityCache, entityCache],
    );

    const { visibleOptions, allOptions, nodesWithChildren } = useTermTreeOptions({
        entities: sourceEntities,
        excludeUrns: existingUrns,
        expandedNodes: isSearching ? undefined : expandedNodes,
        loadingNodeUrns: isSearching ? undefined : fetchingNodes,
    });

    // When nodes are not selectable, disable them in SimpleSelect so they can't be clicked/selected.
    // This prevents dropdown content from changing when clicking on node rows.
    const disabledValues = useMemo(() => {
        if (areNodesSelectable) return [];
        return visibleOptions.filter((o) => o.isNode || o.isLoadingPlaceholder).map((o) => o.value);
    }, [visibleOptions, areNodesSelectable]);

    const combinedOptions = useMemo<TermTreeOption[]>(() => {
        const inDropdown = new Set(allOptions.map((o) => o.value));
        const extras = selectedUrns
            .filter((urn) => !inDropdown.has(urn))
            .map<TermTreeOption>((urn) => {
                const entity = mergedEntityCache[urn];
                if (entity) {
                    const name = entityRegistry.getDisplayName(entity.type, entity);
                    const label = name && !name.startsWith('urn:') ? name : deriveGlossaryLabelFromUrn(urn);
                    return {
                        value: entity.urn,
                        label,
                        entity,
                        color: getGlossaryTermColor(entity as GlossaryTerm, generateColor),
                    };
                }
                return { value: urn, label: deriveGlossaryLabelFromUrn(urn) };
            });
        return [...allOptions, ...extras];
    }, [allOptions, selectedUrns, mergedEntityCache, entityRegistry, generateColor]);

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
                    <LoadingRow $depth={depth} $offsetForCaret>
                        <Loader size="xs" justifyContent="flex-start" alignItems="center" />
                    </LoadingRow>
                );
            }

            // Wrap all options with tag-term-option testid for backward compatibility with E2E tests
            const wrapWithTestId = (content: React.ReactNode) => (
                <div data-testid={`tag-term-option-${option.label}`}>{content}</div>
            );

            if (option.isNode) {
                const hasChildren = nodesWithChildren.has(option.value) || option.isEmptyNode;
                const isExpanded = expandedNodes.has(option.value);
                const CaretIcon = isExpanded ? CaretDown : CaretRight;
                const color = option.color ?? generateColor(option.value);

                // When areNodesSelectable is true, nodes are selectable with checkboxes (like terms).
                // When false, nodes are non-selectable headers for expanding/collapsing only.
                const RowComponent = areNodesSelectable ? OptionRow : NonSelectableRow;

                return wrapWithTestId(
                    <RowComponent $depth={depth} data-testid={`glossary-option-${option.label}`}>
                        <CaretButton
                            type="button"
                            aria-label={
                                isExpanded
                                    ? t('collapseTerm', { label: option.label })
                                    : t('expandTerm', { label: option.label })
                            }
                            aria-expanded={isExpanded}
                            onClick={(e) => {
                                e.stopPropagation();
                                if (hasChildren) handleCaretClick(option.value);
                            }}
                            style={{ visibility: hasChildren ? 'visible' : 'hidden' }}
                        >
                            <CaretIcon size={14} weight="regular" />
                        </CaretButton>
                        {areNodesSelectable ? (
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '8px',
                                    minWidth: 0,
                                    flex: 1,
                                    cursor: 'pointer',
                                }}
                            >
                                <GlossaryColoredIcon color={color} icon={BookmarksSimple} size={20} iconSize={12} />
                                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                    {option.label}
                                </span>
                            </div>
                        ) : (
                            <GlossaryTermPill name={option.label} color={color} icon={BookmarksSimple} />
                        )}
                    </RowComponent>,
                );
            }
            const hasAncestors = (option.ancestorUrns?.length ?? 0) > 0;
            if (option.entity?.type === EntityType.GlossaryTerm) {
                return wrapWithTestId(
                    <OptionRow
                        $depth={depth}
                        $offsetForCaret={hasAncestors}
                        data-testid={`glossary-option-${option.label}`}
                    >
                        <GlossaryTermPill name={option.label} color={option.color ?? generateColor(option.value)} />
                    </OptionRow>,
                );
            }
            return wrapWithTestId(
                <OptionRow
                    $depth={depth}
                    $offsetForCaret={hasAncestors}
                    data-testid={`glossary-option-${option.label}`}
                >
                    <TagTermLabel termName={option.label} />
                </OptionRow>,
            );
        },
        [generateColor, nodesWithChildren, expandedNodes, handleCaretClick, t, areNodesSelectable],
    );

    const renderSelectedValue = useCallback(
        (option: TermTreeOption) => (
            <GlossaryTermPill
                key={option.value}
                name={option.label}
                color={option.color ?? generateColor(option.value)}
                onRemove={() => onUpdate(selectedUrns.filter((urn) => urn !== option.value))}
                dataTestId={`selected-${option.label}`}
            />
        ),
        [selectedUrns, onUpdate, generateColor],
    );

    return (
        <SimpleSelect
            width={width}
            isMultiSelect
            showSearch={showSearch}
            values={selectedUrns}
            placeholder={placeholder}
            options={visibleOptions}
            disabledValues={disabledValues}
            combinedSelectedAndSearchOptions={combinedOptions}
            onUpdate={onUpdate}
            onSearchChange={handleSearch}
            renderCustomOptionText={renderOption}
            renderCustomSelectedValue={renderSelectedValue}
            selectLabelProps={{ variant: 'custom' }}
            isLoading={isSearching ? searchLoading : treeLoading}
            dataTestId={dataTestId}
        />
    );
}
