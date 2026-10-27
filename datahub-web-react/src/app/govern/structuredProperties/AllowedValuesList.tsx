import { Tooltip } from '@components';
import { DndContext, KeyboardSensor, PointerSensor, closestCenter, useSensor, useSensors } from '@dnd-kit/core';
import type { DragEndEvent } from '@dnd-kit/core';
import { restrictToParentElement, restrictToVerticalAxis } from '@dnd-kit/modifiers';
import { SortableContext, sortableKeyboardCoordinates, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { TextAlignLeft } from '@phosphor-icons/react/dist/csr/TextAlignLeft';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useCallback, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import SortableAllowedValue from '@app/govern/structuredProperties/AllowedValuesList.components';
import { AddButtonContainer, ValuesContainer } from '@app/govern/structuredProperties/styledComponents';
import { AllowedValueRow, PropValueField, getAllowedValueKey } from '@app/govern/structuredProperties/utils';
import { Button, Input, TextArea } from '@src/alchemy-components';

const DND_MODIFIERS = [restrictToVerticalAxis, restrictToParentElement];

const NUMBER_INPUT_TYPE = 'number';
const TEXT_INPUT_TYPE = 'text';

type Props = {
    propType: PropValueField;
    isReadOnly: boolean;
    rows: AllowedValueRow[];
    errors?: Record<string, string>;
    addRow: () => void;
    updateRow: (rowId: string, patch: Partial<AllowedValueRow>) => void;
    removeRow: (rowId: string) => void;
    moveRow: (from: number, to: number) => void;
};

const AllowedValuesList = ({ propType, isReadOnly, rows, errors, addRow, updateRow, removeRow, moveRow }: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tc } = useTranslation('common.actions');

    // Rows on which the user opened an empty description field. A row that already has a
    // description is always open and never appears here.
    const [descriptionOverrides, setDescriptionOverrides] = useState<Record<string, boolean>>({});

    const containerRef = useRef<HTMLDivElement>(null);
    const sensors = useSensors(
        useSensor(PointerSensor),
        useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
    );

    const toggleDescription = useCallback((rowKey: string, isShown: boolean) => {
        setDescriptionOverrides((current) => ({ ...current, [rowKey]: !isShown }));
    }, []);

    const handleAdd = () => {
        addRow();
        // Bring the newly added row into view within whichever ancestor is scrolling.
        setTimeout(() => containerRef.current?.lastElementChild?.scrollIntoView({ block: 'nearest' }), 0);
    };

    const handleDragEnd = (event: DragEndEvent) => {
        const { active, over } = event;
        if (!over || active.id === over.id) return;
        const from = rows.findIndex((row) => row.rowId === active.id);
        const to = rows.findIndex((row) => row.rowId === over.id);
        if (from !== -1 && to !== -1) {
            moveRow(from, to);
        }
    };

    return (
        <>
            {rows.length > 0 && (
                <ValuesContainer ref={containerRef}>
                    <DndContext
                        sensors={sensors}
                        collisionDetection={closestCenter}
                        modifiers={DND_MODIFIERS}
                        onDragEnd={handleDragEnd}
                    >
                        <SortableContext items={rows.map((row) => row.rowId)} strategy={verticalListSortingStrategy}>
                            {rows.map((row) => {
                                const rowValue = getAllowedValueKey(row);
                                const isExisting = !!row.isPersisted;

                                const hasDescription = !!row.description;
                                const showDescription =
                                    hasDescription || (!isReadOnly && !!descriptionOverrides[row.rowId]);
                                // An existing description always stays visible, so the toggle only ever adds
                                // one. A saved value's description cannot be edited, so it gets no toggle.
                                const canToggleDescription = !hasDescription && !isExisting;

                                return (
                                    <SortableAllowedValue
                                        key={row.rowId}
                                        id={row.rowId}
                                        isDisabled={isReadOnly}
                                        dragLabel={t('allowedValues.dragTooltip')}
                                        valueInput={
                                            <Tooltip
                                                title={isExisting && t('allowedValues.editExistingTooltip')}
                                                showArrow={false}
                                            >
                                                <div>
                                                    <Input
                                                        label=""
                                                        placeholder={t('allowedValues.valuePlaceholder')}
                                                        type={
                                                            propType === 'numberValue'
                                                                ? NUMBER_INPUT_TYPE
                                                                : TEXT_INPUT_TYPE
                                                        }
                                                        value={rowValue ?? ''}
                                                        setValue={(value) =>
                                                            updateRow(row.rowId, { [propType]: value })
                                                        }
                                                        error={errors?.[row.rowId]}
                                                        isDisabled={isExisting || isReadOnly}
                                                    />
                                                </div>
                                            </Tooltip>
                                        }
                                        descriptionInput={
                                            showDescription ? (
                                                <TextArea
                                                    placeholder={t('allowedValues.descriptionPlaceholder')}
                                                    value={row.description ?? ''}
                                                    onChange={(e) =>
                                                        updateRow(row.rowId, { description: e.target.value })
                                                    }
                                                    isDisabled={isExisting || isReadOnly}
                                                />
                                            ) : undefined
                                        }
                                        actions={
                                            isReadOnly ? undefined : (
                                                <>
                                                    {canToggleDescription && (
                                                        <Tooltip
                                                            title={t(
                                                                showDescription
                                                                    ? 'allowedValues.hideDescriptionTooltip'
                                                                    : 'allowedValues.descriptionTooltip',
                                                            )}
                                                            showArrow={false}
                                                        >
                                                            <Button
                                                                onClick={() =>
                                                                    toggleDescription(row.rowId, showDescription)
                                                                }
                                                                variant="text"
                                                                isCircle
                                                                icon={{ icon: TextAlignLeft, size: 'lg' }}
                                                                aria-label={t(
                                                                    showDescription
                                                                        ? 'allowedValues.hideDescriptionTooltip'
                                                                        : 'allowedValues.descriptionTooltip',
                                                                )}
                                                            />
                                                        </Tooltip>
                                                    )}
                                                    {!isExisting && (
                                                        <Tooltip
                                                            title={t('allowedValues.removeTooltip')}
                                                            showArrow={false}
                                                        >
                                                            <Button
                                                                onClick={() => removeRow(row.rowId)}
                                                                variant="text"
                                                                isCircle
                                                                icon={{ icon: Trash, size: 'lg' }}
                                                                aria-label={t('allowedValues.removeTooltip')}
                                                            />
                                                        </Tooltip>
                                                    )}
                                                </>
                                            )
                                        }
                                    />
                                );
                            })}
                        </SortableContext>
                    </DndContext>
                </ValuesContainer>
            )}

            {!isReadOnly && (
                <AddButtonContainer>
                    <Tooltip title={t('allowedValues.addTooltip')} showArrow={false}>
                        <Button onClick={handleAdd} type="button" variant="text" icon={{ icon: Plus }}>
                            {tc('add')}
                        </Button>
                    </Tooltip>
                </AddButtonContainer>
            )}
        </>
    );
};

export default AllowedValuesList;
