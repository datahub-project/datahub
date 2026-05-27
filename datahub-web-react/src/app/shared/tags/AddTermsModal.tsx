import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { TagTermLabel } from '@app/shared/tags/TagTermLabel';
import { OperationType, isAddOperation, useBatchTagTermMutation } from '@app/shared/tags/useBatchTagTermMutation';
import { useEntityPickerState } from '@app/shared/tags/useEntityPickerState';
import { TermTreeOption, useTermTreeOptions } from '@app/shared/tags/useTermTreeOptions';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal, SimpleSelect } from '@src/alchemy-components';
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

// Node rows are hierarchy headers only — hide the alchemy SimpleSelect's adjacent checkbox so they
// read as static headers rather than disabled options. They render their own caret inline, so they
// don't need the caret-column offset.
const NodeRow = styled(OptionRow)`
    flex: 1;
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

/**
 * Modal that lets the user add (or remove) glossary terms on one or more resources.
 *
 * Uses alchemy `SimpleSelect` in multi-select mode (native checkboxes) with options shaped as a
 * tree (`useTermTreeOptions`): each lineage emits an indented node header + its term children, and
 * the user can expand/collapse subtrees via inline carets. While searching the collapse state is
 * ignored so matching descendants stay visible.
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

    const { urns, setUrns, removeUrn, entityCache, searchText, handleSearch, currentEntities, isLoading } =
        useEntityPickerState({ entityType: EntityType.GlossaryTerm, defaultValues });

    const computeTermColor = useCallback(
        (entity: Entity): string => getGlossaryTermColor(entity as GlossaryTerm, generateColor),
        [generateColor],
    );

    const { visibleOptions, allOptions, disabledValues, nodesWithChildren, collapsedNodes, toggleNodeCollapsed } =
        useTermTreeOptions({
            entities: currentEntities,
            excludeUrns: existingUrns,
            ignoreCollapsed: !!searchText,
        });

    // Always-render the currently-selected URNs as extras so SimpleSelect can render the chip strip
    // even after a search clears the underlying options.
    const combinedOptions = useMemo<TermTreeOption[]>(() => {
        const inDropdown = new Set(allOptions.map((o) => o.value));
        const extras = urns
            .filter((urn) => !inDropdown.has(urn))
            .map<TermTreeOption>((urn) => {
                const entity = entityCache[urn];
                if (entity) {
                    return {
                        value: entity.urn,
                        label: entityRegistry.getDisplayName(entity.type, entity),
                        entity,
                        color: computeTermColor(entity),
                    };
                }
                return { value: urn, label: urn };
            });
        return [...allOptions, ...extras];
    }, [allOptions, urns, entityCache, entityRegistry, computeTermColor]);

    const renderOption = useCallback(
        (option: TermTreeOption) => {
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
        [generateColor, nodesWithChildren, collapsedNodes, toggleNodeCollapsed],
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
                isLoading={isLoading}
                placeholder="Search for glossary term..."
                width="full"
                dataTestId="tag-term-modal-input"
            />
        </Modal>
    );
}
