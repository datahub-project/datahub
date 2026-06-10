import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { deriveGlossaryLabelFromUrn } from '@app/glossaryV2/utils';
import { TagTermLabel } from '@app/shared/tags/TagTermLabel';
import { OperationType, isAddOperation, useBatchTagTermMutation } from '@app/shared/tags/useBatchTagTermMutation';
import { useEntityPickerState } from '@app/shared/tags/useEntityPickerState';
import { useGlossaryTreeEntities } from '@app/shared/tags/useGlossaryTreeEntities';
import { TermTreeOption, useTermTreeOptions } from '@app/shared/tags/useTermTreeOptions';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader, Modal, SimpleSelect } from '@src/alchemy-components';
import { getModalDomContainer } from '@utils/focus';

import { Entity, EntityType, GlossaryTerm, ResourceRefInput } from '@types';

type Props = {
    open: boolean;
    onCloseModal: () => void;
    resources: ResourceRefInput[];
    operationType?: OperationType;
    defaultValues?: { urn: string; entity?: Entity | null }[];
    /** Bypass the mutation entirely — used by `AdvancedFilterSelectValueModal` for filter selection. */
    onOkOverride?: (result: string[]) => void;
    /** URNs already applied to the resource(s); excluded from the dropdown so the user can only ADD new items. */
    existingUrns?: string[];
};

// Caret column (20px) + caret→content gap (8px). Rows under a node get this offset so their pill
// aligns under the parent node's pill rather than the parent's caret.
const CARET_COLUMN_WIDTH = 28;

const OptionRow = styled.div<{ $depth: number; $offsetForCaret?: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    padding-left: ${(props) => props.$depth * 16 + (props.$offsetForCaret ? CARET_COLUMN_WIDTH : 0)}px;
`;

// Base for rows that aren't real selectable options (node headers, loading placeholders): takes
// the full row width and hides the alchemy SimpleSelect's adjacent checkbox via the sibling
// combinator so the row reads as static rather than a disabled option with a greyed-out tick.
const NonSelectableRow = styled(OptionRow)`
    flex: 1;
    & ~ * {
        display: none !important;
    }
`;

// Synthetic loading row inserted by `useTermTreeOptions` while a node's children are being fetched.
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

    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

/**
 * Modal that lets the user add (or remove) glossary terms on one or more resources.
 *
 * Initial state walks the live glossary tree from roots, with lazy-loaded children on expand — the
 * same data flow the `/glossary` sidebar uses (`useGlossaryTreeEntities`). When the user types into
 * the search input, we fall through to the autocomplete path from `useEntityPickerState` and
 * flatten the tree (no expand filtering).
 *
 * The list is rendered through alchemy `SimpleSelect` in multi-select mode (native checkboxes).
 * `useTermTreeOptions` shapes the entities into an indented tree, with node header rows holding
 * inline expand/collapse carets that flip visibility *and* trigger child fetches.
 */
export default function AddTermsModal({
    open,
    onCloseModal,
    resources,
    operationType = OperationType.ADD,
    defaultValues = [],
    onOkOverride,
    existingUrns,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    const { runMutation, disableAction } = useBatchTagTermMutation();

    // Picker state owns selection + the autocomplete search path.
    const {
        urns,
        setUrns,
        removeUrn,
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

    // Selected chips look up display names from either cache. The picker cache covers search /
    // recommendations / defaults; the tree cache covers anything the user clicked into via the
    // browse path (and persists across collapse, unlike `treeEntities`). Picker cache wins on
    // collision so a freshly-loaded autocomplete result trumps a stale tree row.
    const mergedEntityCache = useMemo<Record<string, Entity>>(
        () => ({ ...treeEntityCache, ...entityCache }),
        [treeEntityCache, entityCache],
    );

    const { visibleOptions, allOptions, disabledValues, nodesWithChildren } = useTermTreeOptions({
        entities: sourceEntities,
        excludeUrns: existingUrns,
        // While searching, show every result flat — the user is disambiguating with the text query,
        // not the tree. While browsing, only show rows whose ancestors the user has expanded.
        expandedNodes: isSearching ? undefined : expandedNodes,
        // Drop inline loading rows under any node currently fetching its children. Skipped while
        // searching since the flat result list doesn't have parent rows to hang the loader under.
        loadingNodeUrns: isSearching ? undefined : fetchingNodes,
    });

    // Render the currently-selected URNs as extras so SimpleSelect can render the chip strip
    // even after a search clears the underlying options or a tree branch is collapsed.
    const combinedOptions = useMemo<TermTreeOption[]>(() => {
        const inDropdown = new Set(allOptions.map((o) => o.value));
        const extras = urns
            .filter((urn) => !inDropdown.has(urn))
            .map<TermTreeOption>((urn) => {
                const entity = mergedEntityCache[urn];
                if (entity) {
                    const name = entityRegistry.getDisplayName(entity.type, entity);
                    // `getDisplayName` falls back to the URN if `properties.name`/`name` are missing,
                    // so guard against that leaking into the chip label.
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
    }, [allOptions, urns, mergedEntityCache, entityRegistry, generateColor]);

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
            if (option.isNode) {
                // A node should show a caret if either (a) we already know it has descendant rows
                // (term lineages from the search path), or (b) it's a standalone node entity that
                // has not been expanded yet (browse path — caret triggers a child fetch).
                const hasChildren = nodesWithChildren.has(option.value) || option.isEmptyNode;
                const isExpanded = expandedNodes.has(option.value);
                const CaretIcon = isExpanded ? CaretDown : CaretRight;
                return (
                    <NonSelectableRow $depth={depth} data-testid={`tag-term-option-${option.label}`}>
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
                        <GlossaryTermPill
                            name={option.label}
                            color={option.color ?? generateColor(option.value)}
                            icon={BookmarksSimple}
                        />
                    </NonSelectableRow>
                );
            }
            const hasAncestors = (option.ancestorUrns?.length ?? 0) > 0;
            if (option.entity?.type === EntityType.GlossaryTerm) {
                return (
                    <OptionRow
                        $depth={depth}
                        $offsetForCaret={hasAncestors}
                        data-testid={`tag-term-option-${option.label}`}
                    >
                        <GlossaryTermPill name={option.label} color={option.color ?? generateColor(option.value)} />
                    </OptionRow>
                );
            }
            return (
                <OptionRow
                    $depth={depth}
                    $offsetForCaret={hasAncestors}
                    data-testid={`tag-term-option-${option.label}`}
                >
                    <TagTermLabel termName={option.label} />
                </OptionRow>
            );
        },
        [generateColor, nodesWithChildren, expandedNodes, handleCaretClick],
    );

    const renderSelectedValue = useCallback(
        (option: TermTreeOption) => (
            <GlossaryTermPill
                key={option.value}
                name={option.label}
                color={option.color ?? generateColor(option.value)}
                onRemove={() => removeUrn(option.value)}
                dataTestId={`selected-${option.label}`}
            />
        ),
        [removeUrn, generateColor],
    );

    const onOk = () => {
        if (onOkOverride) {
            onOkOverride(urns);
            return;
        }
        runMutation({
            urns,
            resources,
            type: EntityType.GlossaryTerm,
            operationType,
            onDone: () => {
                onCloseModal();
                setUrns([]);
            },
        });
    };

    const actionLabel = isAddOperation(operationType) ? 'Add' : 'Remove';

    return (
        <Modal
            title={`${actionLabel} Glossary Terms`}
            open={open}
            onCancel={onCloseModal}
            buttons={[
                { text: 'Cancel', variant: 'text', onClick: onCloseModal },
                {
                    text: actionLabel,
                    id: 'addTagButton',
                    buttonDataTestId: 'add-tag-term-from-modal-btn',
                    variant: 'filled',
                    disabled: urns.length === 0 || disableAction,
                    onClick: onOk,
                },
            ]}
            getContainer={getModalDomContainer}
        >
            <SimpleSelect
                isMultiSelect
                showSearch
                showClear={false}
                onSearchChange={handleSearch}
                values={urns}
                onUpdate={setUrns}
                options={visibleOptions}
                disabledValues={disabledValues}
                combinedSelectedAndSearchOptions={combinedOptions}
                renderCustomOptionText={renderOption}
                renderCustomSelectedValue={renderSelectedValue}
                selectLabelProps={{ variant: 'custom' }}
                filterResultsByQuery={false}
                isLoading={isSearching ? searchLoading : treeLoading}
                placeholder="Search for glossary term..."
                width="full"
                dataTestId="tag-term-modal-input"
            />
        </Modal>
    );
}
