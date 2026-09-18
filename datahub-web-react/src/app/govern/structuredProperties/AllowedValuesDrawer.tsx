import { Tooltip } from '@components';
import {
    DndContext,
    DragEndEvent,
    KeyboardSensor,
    PointerSensor,
    closestCenter,
    useSensor,
    useSensors,
} from '@dnd-kit/core';
import { restrictToParentElement, restrictToVerticalAxis } from '@dnd-kit/modifiers';
import { SortableContext, sortableKeyboardCoordinates, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Form, FormInstance, FormListFieldData, FormListOperation } from 'antd';
import React, { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';

import SortableValueRow from '@app/govern/structuredProperties/AllowedValuesDrawer.components';
import {
    AddButtonContainer,
    DeleteIconContainer,
    FieldGroupContainer,
    FormContainer,
    InputLabel,
    StyledDivider,
    ValuesContainer,
} from '@app/govern/structuredProperties/styledComponents';
import { AllowedValueFormRow, PropValueField, getAllowedValueKey } from '@app/govern/structuredProperties/utils';
import { Button, Icon, Input, Text, TextArea } from '@src/alchemy-components';

const VALIDATE_TRIGGERS = ['onChange', 'onBlur'];

// A small distance keeps the handle clickable for focus without starting a drag on an accidental
// pointer jitter.
const DRAG_ACTIVATION_DISTANCE = 4;

type Props = {
    showAllowedValuesDrawer: boolean;
    propType: PropValueField;
    allowedValues: AllowedValueFormRow[] | undefined;
    isEditMode: boolean;
    existingValueKeys: Set<string | number>;
    form: FormInstance;
};

const AllowedValuesDrawer = ({
    showAllowedValuesDrawer,
    propType,
    allowedValues,
    isEditMode,
    existingValueKeys,
    form,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');

    useEffect(() => {
        form.setFieldsValue({ allowedValues: allowedValues || [{}] });
    }, [form, showAllowedValuesDrawer, allowedValues]);

    const containerRef = useRef<HTMLDivElement>(null);

    // Scroll to the bottom to show the newly added fields
    const scrollToBottom = () => {
        if (containerRef.current) {
            containerRef.current.scrollTop = containerRef.current.scrollHeight;
        }
    };

    const sensors = useSensors(
        useSensor(PointerSensor, { activationConstraint: { distance: DRAG_ACTIVATION_DISTANCE } }),
        useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
    );

    // Rows are watched so a value can be matched back to the saved definition by its value rather
    // than by its position, which stops being stable once the list can be reordered.
    const rows: AllowedValueFormRow[] = Form.useWatch('allowedValues', form) ?? [];

    const handleDragEnd = (event: DragEndEvent, fields: FormListFieldData[], move: FormListOperation['move']): void => {
        const { active, over } = event;
        if (!over || active.id === over.id) return;

        const from = fields.findIndex((field) => String(field.key) === active.id);
        const to = fields.findIndex((field) => String(field.key) === over.id);
        if (from === -1 || to === -1) return;

        move(from, to);
    };

    return (
        <Form form={form}>
            <Form.List name="allowedValues">
                {(fields, { add, remove, move }) => (
                    <FormContainer>
                        {fields.length > 0 && (
                            <ValuesContainer ref={containerRef} height={window.innerHeight}>
                                <DndContext
                                    sensors={sensors}
                                    collisionDetection={closestCenter}
                                    modifiers={[restrictToVerticalAxis, restrictToParentElement]}
                                    onDragEnd={(event) => handleDragEnd(event, fields, move)}
                                >
                                    <SortableContext
                                        items={fields.map((field) => String(field.key))}
                                        strategy={verticalListSortingStrategy}
                                    >
                                        {fields.map((field, index) => {
                                            const valueKey = getAllowedValueKey(rows[field.name] ?? {});
                                            const isExisting =
                                                isEditMode &&
                                                valueKey !== undefined &&
                                                valueKey !== null &&
                                                existingValueKeys.has(valueKey);

                                            return (
                                                <React.Fragment key={field.key}>
                                                    <SortableValueRow
                                                        id={String(field.key)}
                                                        isDisabled={fields.length < 2}
                                                        dragLabel={t('allowedValues.dragLabel')}
                                                    >
                                                        <FieldGroupContainer>
                                                            <InputLabel>
                                                                {t('allowedValues.valueLabel')}
                                                                <Text color="red" weight="bold">
                                                                    *
                                                                </Text>
                                                            </InputLabel>
                                                            <Tooltip
                                                                title={
                                                                    isExisting && t('allowedValues.editExistingTooltip')
                                                                }
                                                                showArrow={false}
                                                            >
                                                                <Form.Item
                                                                    {...field}
                                                                    name={[field.name, propType]}
                                                                    rules={[
                                                                        {
                                                                            required: true,
                                                                            message: t('allowedValues.valueError'),
                                                                        },
                                                                    ]}
                                                                    key={`${field.name}.value`}
                                                                    validateTrigger={VALIDATE_TRIGGERS}
                                                                >
                                                                    <Input
                                                                        label=""
                                                                        placeholder={t(
                                                                            'allowedValues.valuePlaceholder',
                                                                        )}
                                                                        type={
                                                                            propType === 'numberValue'
                                                                                ? 'number'
                                                                                : 'text'
                                                                        }
                                                                        isDisabled={isExisting}
                                                                    />
                                                                </Form.Item>
                                                            </Tooltip>
                                                            <Form.Item
                                                                {...field}
                                                                name={[field.name, 'description']}
                                                                key={`${field.name}.desc`}
                                                            >
                                                                <TextArea
                                                                    label={tl('description')}
                                                                    placeholder={t(
                                                                        'allowedValues.descriptionPlaceholder',
                                                                    )}
                                                                    isDisabled={isExisting}
                                                                />
                                                            </Form.Item>
                                                            {!isExisting && (
                                                                <DeleteIconContainer>
                                                                    <Tooltip
                                                                        title={t('allowedValues.removeTooltip')}
                                                                        showArrow={false}
                                                                    >
                                                                        <Icon
                                                                            icon={Trash}
                                                                            onClick={() => remove(field.name)}
                                                                            color="gray"
                                                                            size="xl"
                                                                        />
                                                                    </Tooltip>
                                                                </DeleteIconContainer>
                                                            )}
                                                        </FieldGroupContainer>
                                                    </SortableValueRow>
                                                    {index < fields.length - 1 && <StyledDivider />}
                                                </React.Fragment>
                                            );
                                        })}
                                    </SortableContext>
                                </DndContext>
                            </ValuesContainer>
                        )}

                        <AddButtonContainer>
                            <Tooltip title={t('allowedValues.addTooltip')} showArrow={false}>
                                <Button
                                    onClick={() => {
                                        add();
                                        setTimeout(() => scrollToBottom(), 0);
                                    }}
                                    type="button"
                                >
                                    {tc('add')}
                                </Button>
                            </Tooltip>
                        </AddButtonContainer>
                    </FormContainer>
                )}
            </Form.List>
        </Form>
    );
};

export default AllowedValuesDrawer;
