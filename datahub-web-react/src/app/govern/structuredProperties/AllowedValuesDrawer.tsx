import { Button, Icon, Input, Text, TextArea } from '@src/alchemy-components';
import { AllowedValue } from '@src/types.generated';
import { Form, FormInstance } from 'antd';
import { Tooltip } from '@components';
import React, { useEffect, useRef } from 'react';
import {
    AddButtonContainer,
    DeleteIconContainer,
    FieldGroupContainer,
    FormContainer,
    InputLabel,
    StyledDivider,
    ValuesContainer,
} from './styledComponents';
import { PropValueField } from './utils';

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
                                                Value
                                                <Text color="red" weight="bold">
                                                    *
                                                </Text>
                                            </InputLabel>
                                            <Tooltip
                                                title={isExisting && 'Editing existing allowed values is not permitted'}
                                                showArrow={false}
                                            >
                                                <Form.Item
                                                    {...field}
                                                    name={[field.name, propType]}
                                                    rules={[
                                                        {
                                                            required: true,
                                                            message: 'Please enter the value!',
                                                        },
                                                    ]}
                                                    key={`${field.name}.value`}
                                                    validateTrigger={['onChange', 'onBlur']}
                                                >
                                                    <Input
                                                        label=""
                                                        placeholder="Provide a value"
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
                                                    label="Description"
                                                    placeholder="Provide a description"
                                                    isDisabled={isExisting}
                                                />
                                            </Form.Item>
                                            {!isExisting && (
                                                <DeleteIconContainer>
                                                    <Tooltip
                                                        title="Remove from the allowed values list"
                                                        showArrow={false}
                                                    >
                                                        <Icon
                                                            icon="Delete"
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
                            <Tooltip title="Add a new value to the allowed list" showArrow={false}>
                                <Button
                                    onClick={() => {
                                        add();
                                        setTimeout(() => scrollToBottom(), 0);
                                    }}
                                    color="violet"
                                    type="button"
                                >
                                    Add
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
