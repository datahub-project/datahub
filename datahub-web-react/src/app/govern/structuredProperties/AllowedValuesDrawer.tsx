import { Tooltip } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Form, FormInstance } from 'antd';
import React, { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';

import {
    AddButtonContainer,
    DeleteIconContainer,
    FieldGroupContainer,
    FormContainer,
    InputLabel,
    StyledDivider,
    ValuesContainer,
} from '@app/govern/structuredProperties/styledComponents';
import { PropValueField } from '@app/govern/structuredProperties/utils';
import { Button, Icon, Input, Text, TextArea } from '@src/alchemy-components';
import { AllowedValue } from '@src/types.generated';

const VALIDATE_TRIGGERS = ['onChange', 'onBlur'];

interface Props {
    showAllowedValuesDrawer: boolean;
    propType: PropValueField;
    allowedValues: AllowedValue[] | undefined;
    isEditMode: boolean;
    noOfExistingValues: number;
    form: FormInstance;
}

const AllowedValuesDrawer = ({
    showAllowedValuesDrawer,
    propType,
    allowedValues,
    isEditMode,
    noOfExistingValues,
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

    return (
        <Form form={form}>
            <Form.List name="allowedValues">
                {(fields, { add, remove }) => (
                    <FormContainer>
                        {fields.length > 0 && (
                            <ValuesContainer ref={containerRef} height={window.innerHeight}>
                                {fields.map((field, index) => {
                                    const isExisting = isEditMode && index < noOfExistingValues;

                                    return (
                                        <FieldGroupContainer key={field.name}>
                                            <InputLabel>
                                                {t('allowedValues.valueLabel')}
                                                <Text color="red" weight="bold">
                                                    *
                                                </Text>
                                            </InputLabel>
                                            <Tooltip
                                                title={isExisting && t('allowedValues.editExistingTooltip')}
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
                                                        placeholder={t('allowedValues.valuePlaceholder')}
                                                        type={propType === 'numberValue' ? 'number' : 'text'}
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
                                                    placeholder={t('allowedValues.descriptionPlaceholder')}
                                                    isDisabled={isExisting}
                                                />
                                            </Form.Item>
                                            {!isExisting && (
                                                <DeleteIconContainer>
                                                    <Tooltip title={t('allowedValues.removeTooltip')} showArrow={false}>
                                                        <Icon
                                                            icon={Trash}
                                                            onClick={() => remove(field.name)}
                                                            color="gray"
                                                            size="xl"
                                                        />
                                                    </Tooltip>
                                                </DeleteIconContainer>
                                            )}
                                            {index < fields.length - 1 && <StyledDivider />}
                                        </FieldGroupContainer>
                                    );
                                })}
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
