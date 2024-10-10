import { Button, colors, Icon, Input, Text, TextArea } from '@src/alchemy-components';
import { StyledButton } from '@src/app/shared/share/v2/styledComponents';
import { AllowedValue } from '@src/types.generated';
import { Form, Tooltip } from 'antd';
import React, { useEffect, useRef } from 'react';
import {
    AddButtonContainer,
    DeleteIconContainer,
    FieldGroupContainer,
    FormContainer,
    InputLabel,
    ModalFooter,
    StyledDivider,
    StyledModal,
    ValuesContainer,
} from './styledComponents';
import { PropValueField } from './utils';

interface Props {
    isOpen: boolean;
    showAllowedValuesModal: boolean;
    setShowAllowedValuesModal: React.Dispatch<React.SetStateAction<boolean>>;
    propType: PropValueField;
    allowedValues: AllowedValue[] | undefined;
    setAllowedValues: React.Dispatch<React.SetStateAction<AllowedValue[] | undefined>>;
    isEditMode: boolean;
    noOfExistingValues: number;
}

const AllowedValuesModal = ({
    isOpen,
    showAllowedValuesModal,
    setShowAllowedValuesModal,
    propType,
    allowedValues,
    setAllowedValues,
    isEditMode,
    noOfExistingValues,
}: Props) => {
    const [form] = Form.useForm();

    useEffect(() => {
        form.setFieldsValue({ allowedValues: allowedValues || [{}] });
    }, [form, showAllowedValuesModal, allowedValues]);

    const handleModalClose = () => {
        setShowAllowedValuesModal(false);
        form.resetFields();
    };

    const handleUpdateValues = () => {
        form.validateFields().then(() => {
            setAllowedValues(form.getFieldValue('allowedValues'));
            setShowAllowedValuesModal(false);
        });
    };

    const containerRef = useRef<HTMLDivElement>(null);

    // Scroll to the bottom to show the newly added fields
    const scrollToBottom = () => {
        if (containerRef.current) {
            containerRef.current.scrollTop = containerRef.current.scrollHeight;
        }
    };

    return (
        <StyledModal
            open={isOpen}
            onCancel={handleModalClose}
            centered
            title={
                <>
                    <Text size="lg" color="gray" weight="bold">
                        Update Allowed Values
                    </Text>
                    <Text color="gray">Define the values that are valid for this structured property</Text>
                </>
            }
            footer={
                <ModalFooter>
                    <Button variant="text" onClick={handleModalClose}>
                        Cancel
                    </Button>
                    <Button onClick={handleUpdateValues}>Update</Button>
                </ModalFooter>
            }
            destroyOnClose
        >
            <Form form={form}>
                <Form.List name="allowedValues">
                    {(fields, { add, remove }) => (
                        <FormContainer>
                            {fields.length > 0 && (
                                <ValuesContainer ref={containerRef}>
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
                                                    title={
                                                        isExisting && 'Editing existing allowed values is not permitted'
                                                    }
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
                                    <StyledButton
                                        onClick={() => {
                                            add();
                                            setTimeout(() => scrollToBottom(), 0);
                                        }}
                                        $color={colors.violet[500]}
                                    >
                                        Add
                                    </StyledButton>
                                </Tooltip>
                            </AddButtonContainer>
                        </FormContainer>
                    )}
                </Form.List>
            </Form>
        </StyledModal>
    );
};

export default AllowedValuesModal;
