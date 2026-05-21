import { toast } from '@components';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { handleBatchError } from '@app/entity/shared/utils';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { TagTermLabel } from '@app/shared/tags/TagTermLabel';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal, SimpleSelect } from '@src/alchemy-components';
import { SelectOption } from '@src/alchemy-components/components/Select/types';
import { getModalDomContainer } from '@utils/focus';

import {
    useBatchAddTagsMutation,
    useBatchAddTermsMutation,
    useBatchRemoveTagsMutation,
    useBatchRemoveTermsMutation,
} from '@graphql/mutations.generated';
import { useGetAutoCompleteResultsLazyQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, Entity, EntityType, GlossaryNode, GlossaryTerm, ResourceRefInput, Tag } from '@types';

export enum OperationType {
    ADD,
    REMOVE,
}

type EditTagsModalProps = {
    open: boolean;
    onCloseModal: () => void;
    resources: ResourceRefInput[];
    type?: EntityType;
    operationType?: OperationType;
    defaultValues?: { urn: string; entity?: Entity | null }[];
    onOkOverride?: (result: string[]) => void;
    // URNs already applied to the resource(s); excluded from the dropdown so the user can only ADD new items.
    existingUrns?: string[];
};

// Kept exported for other modals that still use the V1 browser (PolicyPrivilegeForm, AddRelatedTermsModal).
export const BrowserWrapper = styled.div<{
    isHidden: boolean;
    width?: string;
    maxHeight?: number;
    minWidth?: number;
    maxWidth?: number;
}>`
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    max-height: ${(props) => (props.maxHeight ? props.maxHeight : '380')}px;
    overflow: auto;
    position: absolute;
    transition: opacity 0.2s;
    width: ${(props) => (props.width ? props.width : '100%')};
    ${(props) => props.minWidth !== undefined && `min-width: ${props.minWidth}px;`}
    ${(props) => props.maxWidth !== undefined && `max-width: ${props.maxWidth}px;`}
    z-index: 1051;
    ${(props) =>
        props.isHidden &&
        `
        opacity: 0;
        height: 0;
    `}
`;

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

// Node rows are hierarchy headers only — hide the alchemy SimpleSelect's adjacent checkbox so they
// read as static headers rather than disabled options. They render their own caret inline, so they
// don't need the caret-column offset.
const NodeRow = styled(OptionRow)`
    flex: 1;
    & ~ * {
        display: none !important;
    }
`;

interface EntityOption extends SelectOption {
    entity?: Entity;
    depth?: number;
    isNode?: boolean;
    color?: string;
    /** URNs of any parent nodes (root → direct-parent order). Used for expand/collapse filtering. */
    ancestorUrns?: string[];
}

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

const isAddOperation = (op?: OperationType) => op === OperationType.ADD || op === undefined;

export default function EditTagTermsModal({
    open,
    onCloseModal,
    resources,
    type = EntityType.Tag,
    operationType = OperationType.ADD,
    defaultValues = [],
    onOkOverride,
    existingUrns,
}: EditTagsModalProps) {
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType } = useReloadableContext();

    const [urns, setUrns] = useState<string[]>(defaultValues.map((v) => v.urn));
    const [entityCache, setEntityCache] = useState<Record<string, Entity>>(() => {
        const cache: Record<string, Entity> = {};
        defaultValues.forEach(({ urn, entity }) => {
            if (entity) cache[urn] = entity;
        });
        return cache;
    });
    const [searchText, setSearchText] = useState('');
    const [disableAction, setDisableAction] = useState(false);

    const [batchAddTagsMutation] = useBatchAddTagsMutation();
    const [batchRemoveTagsMutation] = useBatchRemoveTagsMutation();
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchRemoveTermsMutation] = useBatchRemoveTermsMutation();

    const [tagTermSearch, { data: searchData, loading: searchLoading }] = useGetAutoCompleteResultsLazyQuery();
    // Seed the dropdown with the top entities of the relevant type so the modal isn't empty before the user types.
    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations([type]);

    const handleSearch = useCallback(
        (text: string) => {
            const trimmed = text.trim();
            setSearchText(trimmed);
            if (trimmed.length > 0) {
                tagTermSearch({
                    variables: {
                        input: { type, query: trimmed, limit: 10 },
                    },
                });
            }
        },
        [tagTermSearch, type],
    );

    const searchEntities = useMemo<Entity[]>(
        () => (searchData?.autoComplete?.entities as Entity[] | undefined) || [],
        [searchData],
    );
    const initialEntities = useMemo<Entity[]>(
        () => (!searchText ? (recommendedData as Entity[] | undefined) || [] : []),
        [searchText, recommendedData],
    );
    const currentEntities = searchText ? searchEntities : initialEntities;

    // Cache entities we've seen so the selected-label area can still render them after the search clears.
    useEffect(() => {
        if (currentEntities.length === 0) return;
        setEntityCache((prev) => {
            const next = { ...prev };
            let changed = false;
            currentEntities.forEach((e) => {
                if (!next[e.urn]) {
                    next[e.urn] = e;
                    changed = true;
                }
            });
            return changed ? next : prev;
        });
    }, [currentEntities]);

    const generateColor = useGenerateGlossaryColorFromPalette();

    const computeTermColor = useCallback(
        (entity: Entity): string => getGlossaryTermColor(entity as GlossaryTerm, generateColor),
        [generateColor],
    );

    const toTagOption = useCallback(
        (entity: Entity): EntityOption => ({
            value: entity.urn,
            label: (entity as Tag).name || entityRegistry.getDisplayName(entity.type, entity),
            entity,
            depth: 0,
            color: (entity as Tag).properties?.colorHex || undefined,
        }),
        [entityRegistry],
    );

    const excludeSet = useMemo(() => new Set(existingUrns || []), [existingUrns]);

    // Build a tree-ordered flat list of options for glossary terms.
    // Each term carries its parent nodes; we synthesise a row per unique parent node so the dropdown
    // reads like the sidebar tree (node header → indented term children). Node rows are disabled so
    // the user can't accidentally select a node (we're only adding terms). Already-applied terms are
    // dropped, and node headers are only emitted if they have at least one selectable child.
    const termTreeOptions = useMemo<EntityOption[]>(() => {
        const result: EntityOption[] = [];
        const seenNodeUrns = new Set<string>();
        const groupedByLineage = new Map<string, GlossaryTerm[]>();
        const lineageOrder: string[] = [];

        currentEntities.forEach((entity) => {
            if (entity.type !== EntityType.GlossaryTerm) return;
            if (excludeSet.has(entity.urn)) return;
            const term = entity as GlossaryTerm;
            const lineage = (term.parentNodes?.nodes || []).map((n) => n.urn).join('/');
            if (!groupedByLineage.has(lineage)) {
                groupedByLineage.set(lineage, []);
                lineageOrder.push(lineage);
            }
            groupedByLineage.get(lineage)!.push(term);
        });

        lineageOrder.forEach((lineage) => {
            const terms = groupedByLineage.get(lineage)!;
            // ParentNodes are direct-parent-first; reverse to render root → leaf.
            const nodes = [...(terms[0].parentNodes?.nodes || [])].reverse();
            nodes.forEach((node, depth) => {
                if (seenNodeUrns.has(node.urn)) return;
                seenNodeUrns.add(node.urn);
                const nodeColor = (node as GlossaryNode).displayProperties?.colorHex || generateColor(node.urn);
                result.push({
                    value: node.urn,
                    label: entityRegistry.getDisplayName(node.type, node),
                    isNode: true,
                    depth,
                    color: nodeColor,
                    ancestorUrns: nodes.slice(0, depth).map((n) => n.urn),
                });
            });
            const termDepth = nodes.length;
            const rootNode = nodes[0];
            const termColor = rootNode
                ? (rootNode as GlossaryNode).displayProperties?.colorHex || generateColor(rootNode.urn)
                : undefined;
            const termAncestorUrns = nodes.map((n) => n.urn);
            terms.forEach((term) => {
                result.push({
                    value: term.urn,
                    label: entityRegistry.getDisplayName(term.type, term),
                    entity: term,
                    depth: termDepth,
                    color: termColor || generateColor(term.urn),
                    ancestorUrns: termAncestorUrns,
                });
            });
        });

        return result;
    }, [currentEntities, entityRegistry, generateColor, excludeSet]);

    const dropdownOptions = useMemo<EntityOption[]>(() => {
        if (type === EntityType.Tag) {
            const tagOptions = currentEntities.map(toTagOption);
            return excludeSet.size === 0 ? tagOptions : tagOptions.filter((option) => !excludeSet.has(option.value));
        }
        return termTreeOptions;
    }, [type, currentEntities, toTagOption, termTreeOptions, excludeSet]);

    // Node URNs that have at least one descendant row — only these need an expand/collapse caret.
    const nodesWithChildren = useMemo(() => {
        const withChildren = new Set<string>();
        dropdownOptions.forEach((opt) => {
            (opt.ancestorUrns || []).forEach((urn) => withChildren.add(urn));
        });
        return withChildren;
    }, [dropdownOptions]);

    const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(new Set());

    const toggleNodeCollapsed = useCallback((nodeUrn: string) => {
        setCollapsedNodes((prev) => {
            const next = new Set(prev);
            if (next.has(nodeUrn)) next.delete(nodeUrn);
            else next.add(nodeUrn);
            return next;
        });
    }, []);

    // While searching we ignore collapse state so matching descendants stay visible.
    const visibleDropdownOptions = useMemo(() => {
        if (type !== EntityType.GlossaryTerm || searchText || collapsedNodes.size === 0) return dropdownOptions;
        return dropdownOptions.filter((opt) => {
            const ancestors = opt.ancestorUrns || [];
            return !ancestors.some((urn) => collapsedNodes.has(urn));
        });
    }, [dropdownOptions, collapsedNodes, type, searchText]);

    // Node options are present for hierarchy only — they're not valid selections.
    const disabledValues = useMemo(
        () => dropdownOptions.filter((o) => o.isNode).map((o) => o.value),
        [dropdownOptions],
    );

    const combinedOptions = useMemo<EntityOption[]>(() => {
        const inDropdown = new Set(dropdownOptions.map((o) => o.value));
        const extras = urns
            .filter((urn) => !inDropdown.has(urn))
            .map<EntityOption>((urn) => {
                const entity = entityCache[urn];
                if (entity) {
                    if (type === EntityType.Tag) return toTagOption(entity);
                    return {
                        value: entity.urn,
                        label: entityRegistry.getDisplayName(entity.type, entity),
                        entity,
                        color: computeTermColor(entity),
                    };
                }
                return { value: urn, label: urn };
            });
        return [...dropdownOptions, ...extras];
    }, [dropdownOptions, urns, entityCache, type, toTagOption, entityRegistry, computeTermColor]);

    const renderOption = useCallback(
        (option: EntityOption) => {
            const depth = option.depth || 0;
            if (option.isNode) {
                const hasChildren = nodesWithChildren.has(option.value);
                const isCollapsed = collapsedNodes.has(option.value);
                const CaretIcon = isCollapsed ? CaretRight : CaretDown;
                return (
                    <NodeRow $depth={depth} data-testid={`tag-term-option-${option.label}`}>
                        <CaretButton
                            type="button"
                            aria-label={isCollapsed ? `Expand ${option.label}` : `Collapse ${option.label}`}
                            aria-expanded={!isCollapsed}
                            // Stops alchemy SimpleSelect from treating this as an option click.
                            onMouseDown={(e) => e.preventDefault()}
                            onClick={(e) => {
                                e.stopPropagation();
                                if (hasChildren) toggleNodeCollapsed(option.value);
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
                    </NodeRow>
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
            if (option.entity?.type === EntityType.Tag || type === EntityType.Tag) {
                return (
                    <OptionRow $depth={depth} data-testid={`tag-term-option-${option.label}`}>
                        <TagPill name={option.label} color={option.color} colorHash={option.value} />
                    </OptionRow>
                );
            }
            // Fallback (term name with no entity loaded yet)
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
        [type, generateColor, nodesWithChildren, collapsedNodes, toggleNodeCollapsed],
    );

    const removeUrn = useCallback((urn: string) => setUrns((prev) => prev.filter((u) => u !== urn)), []);

    // Selected items render in their respective sidebar pill format so the modal selection reads
    // identically to the rest of the app.
    const renderSelectedValue = useCallback(
        (option: EntityOption) => {
            if (type === EntityType.Tag) {
                return (
                    <TagPill
                        key={option.value}
                        name={option.label}
                        color={option.color}
                        colorHash={option.value}
                        onRemove={() => removeUrn(option.value)}
                        dataTestId={`selected-${option.label}`}
                    />
                );
            }
            return (
                <GlossaryTermPill
                    key={option.value}
                    name={option.label}
                    color={option.color ?? generateColor(option.value)}
                    onRemove={() => removeUrn(option.value)}
                    dataTestId={`selected-${option.label}`}
                />
            );
        },
        [type, removeUrn, generateColor],
    );

    const sendAnalytics = useCallback(() => {
        const isSchemaField = resources[0].subResource;

        let eventType;
        if (isSchemaField) {
            eventType =
                type === EntityType.Tag ? EntityActionType.UpdateSchemaTags : EntityActionType.UpdateSchemaTerms;
        } else {
            eventType = type === EntityType.Tag ? EntityActionType.UpdateTags : EntityActionType.UpdateTerms;
        }
        const isBatchAdd = resources.length > 1;
        if (isBatchAdd) {
            analytics.event({
                type: EventType.BatchEntityActionEvent,
                actionType: eventType,
                entityUrns: resources.map((resource) => resource.resourceUrn),
            });
        } else {
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: eventType,
                entityType: type,
                entityUrn: resources[0].resourceUrn,
            });
        }
    }, [resources, type]);

    const finishMutation = useCallback(() => {
        setDisableAction(false);
        onCloseModal();
        setUrns([]);
    }, [onCloseModal]);

    const showBatchError = useCallback(
        (e: Error, defaultMessage: string) => {
            const { content, duration } = handleBatchError(urns, e, {
                content: defaultMessage,
                duration: 3,
            });
            toast.error(content, { duration });
        },
        [urns],
    );

    const batchAddTags = () => {
        batchAddTagsMutation({
            variables: { input: { tagUrns: urns, resources } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success('Added Tags!', { duration: 2 });
                    sendAnalytics();
                }
            })
            .catch((e) => {
                showBatchError(e, `Failed to add: \n ${e.message || ''}`);
            })
            .finally(finishMutation);
    };

    const batchAddTerms = () => {
        batchAddTermsMutation({
            variables: { input: { termUrns: urns, resources } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success('Added Terms!', { duration: 2 });
                    sendAnalytics();
                    reloadByKeyType(
                        [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)],
                        3000,
                    );
                }
            })
            .catch((e) => {
                showBatchError(e, `Failed to add: \n ${e.message || ''}`);
            })
            .finally(finishMutation);
    };

    const batchRemoveTags = () => {
        batchRemoveTagsMutation({
            variables: { input: { tagUrns: urns, resources } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success('Removed Tags!', { duration: 2 });
                }
            })
            .catch((e) => {
                showBatchError(e, `Failed to remove: \n ${e.message || ''}`);
            })
            .finally(finishMutation);
    };

    const batchRemoveTerms = () => {
        batchRemoveTermsMutation({
            variables: { input: { termUrns: urns, resources } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success('Removed Terms!', { duration: 2 });
                    reloadByKeyType(
                        [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)],
                        3000,
                    );
                }
            })
            .catch((e) => {
                showBatchError(e, `Failed to remove: \n ${e.message || ''}`);
            })
            .finally(finishMutation);
    };

    const onOk = () => {
        if (onOkOverride) {
            onOkOverride(urns);
            return;
        }
        if (!resources) {
            onCloseModal();
            return;
        }
        setDisableAction(true);

        if (type === EntityType.Tag) {
            if (isAddOperation(operationType)) batchAddTags();
            else batchRemoveTags();
        } else if (isAddOperation(operationType)) {
            batchAddTerms();
        } else {
            batchRemoveTerms();
        }
    };

    const entityName = entityRegistry.getEntityName(type);
    const actionLabel = isAddOperation(operationType) ? 'Add' : 'Remove';

    return (
        <Modal
            title={`${actionLabel} ${entityName}s`}
            open={open}
            onCancel={onCloseModal}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onCloseModal,
                },
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
                options={visibleDropdownOptions}
                disabledValues={disabledValues}
                combinedSelectedAndSearchOptions={combinedOptions}
                renderCustomOptionText={renderOption}
                renderCustomSelectedValue={renderSelectedValue}
                selectLabelProps={{ variant: 'custom' }}
                filterResultsByQuery={false}
                isLoading={searchLoading || recommendationsLoading}
                placeholder={`Search for ${entityName?.toLowerCase()}...`}
                width="full"
                dataTestId="tag-term-modal-input"
            />
        </Modal>
    );
}
