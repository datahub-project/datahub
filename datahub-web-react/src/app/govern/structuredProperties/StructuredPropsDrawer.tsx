import { Button, Input, SimpleSelect, Text, TextArea } from '@src/alchemy-components';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useCreateStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import { EntityType, PropertyCardinality } from '@src/types.generated';
import { Form } from 'antd';
import React, { useState } from 'react';
import AdvancedOptions from './AdvancedOptions';
import { DrawerHeader, FieldLabel, FooterContainer, RowContainer, StyledDrawer, StyledIcon } from './styledComponents';
import { APPLIES_TO_ENTITIES, getEntityTypeUrn, SEARCHABLE_ENTITY_TYPES, valueTypes } from './utils';

interface Props {
    isDrawerOpen: boolean;
    setIsDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    refetch: () => void;
}

const StructuredPropsDrawer = ({ isDrawerOpen, setIsDrawerOpen, refetch }: Props) => {
    const [form] = Form.useForm();

    const entityRegistry = useEntityRegistryV2();

    const [selectedValueType, setSelectedValueType] = useState<string>('');
    const [cardinality, setCardinality] = useState<PropertyCardinality>(PropertyCardinality.Single);

    const getEntitiesListOptions = (entitiesList: EntityType[]) => {
        const listOptions: { label: string; value: string }[] = [];
        entitiesList.forEach((type) => {
            const entity = {
                label: entityRegistry.getEntityName(type) || '',
                value: getEntityTypeUrn(type),
            };
            listOptions.push(entity);
        });
        return listOptions;
    };

    const [createStructuredProperty] = useCreateStructuredPropertyMutation();

    const handleClose = () => {
        setIsDrawerOpen(false);
        form.resetFields();
    };

    const showErrorMessage = () => {
        showToastMessage(ToastType.ERROR, `Failed to create structured property.`, 3);
    };

    const showSuccessMessage = () => {
        showToastMessage(ToastType.SUCCESS, `Structured property created!`, 3);
    };

    const handleSelectChange = (field, values) => {
        form.setFieldValue(field, values);
    };

    const handleTypeUpdate = (values: string[]) => {
        const typeOption = valueTypes.find((type) => type.value === values[0]);
        const typeUrn = typeOption?.key || '';
        setSelectedValueType(typeUrn);
        handleSelectChange('valueType', typeUrn);

        const isList = typeOption?.label.includes('List');
        if (isList) setCardinality(PropertyCardinality.Multiple);
        else setCardinality(PropertyCardinality.Single);
    };

    const onSubmit = () => {
        const formData = form.getFieldsValue();

        // Add default qualified name based on the displayName
        if (!formData.qualifiedName)
            form.setFieldValue('qualifiedName', formData.displayName?.replace(/\s/g, '').toLowerCase());

        form.validateFields().then(() => {
            createStructuredProperty({
                variables: {
                    input: {
                        ...form.getFieldsValue(),
                        cardinality,
                    },
                },
            })
                .then(() => {
                    showSuccessMessage();
                    refetch();
                })
                .catch(() => {
                    showErrorMessage();
                })
                .finally(() => {
                    form.resetFields();
                    setIsDrawerOpen(false);
                });
        });
    };

    return (
        <StyledDrawer
            open={isDrawerOpen}
            closable={false}
            width={480}
            title={
                <DrawerHeader>
                    <Text color="gray" weight="bold">
                        Create Structured Property
                    </Text>
                    <StyledIcon icon="Close" color="gray" onClick={handleClose} />
                </DrawerHeader>
            }
            footer={
                <FooterContainer>
                    <Button style={{ display: 'block', width: '100%' }} onClick={onSubmit}>
                        Create
                    </Button>
                </FooterContainer>
            }
        >
            <Form form={form}>
                <Form.Item
                    name="displayName"
                    rules={[
                        {
                            required: true,
                            message: 'Please enter the name',
                        },
                    ]}
                >
                    <Input label="Name" placeholder="Enter name" />
                </Form.Item>
                <Form.Item name="description">
                    <TextArea label="Description" placeholder="Add description here" />
                </Form.Item>
                <RowContainer>
                    <FieldLabel> Property Type</FieldLabel>
                    <Form.Item
                        name="valueType"
                        rules={[
                            {
                                required: true,
                                message: 'Please select the property type',
                            },
                        ]}
                    >
                        <SimpleSelect
                            options={valueTypes}
                            onUpdate={(values) => handleTypeUpdate(values)}
                            placeholder="Select Property Type"
                        />
                    </Form.Item>
                </RowContainer>
                {selectedValueType === 'urn:li:dataType:datahub.urn' && (
                    <RowContainer>
                        <FieldLabel> Allowed Entity Types</FieldLabel>
                        <Form.Item name={['typeQualifier', 'allowedTypes']}>
                            <SimpleSelect
                                options={getEntitiesListOptions(SEARCHABLE_ENTITY_TYPES)}
                                onUpdate={(values) => handleSelectChange(['typeQualifier', 'allowedTypes'], values)}
                                placeholder="Select Allowed Entity Types"
                                isMultiSelect
                            />
                        </Form.Item>
                    </RowContainer>
                )}
                <RowContainer>
                    <FieldLabel> Applies to</FieldLabel>

                    <Form.Item
                        name="entityTypes"
                        rules={[
                            {
                                required: true,
                                message: 'Please select the applies to entities',
                            },
                        ]}
                    >
                        <SimpleSelect
                            options={getEntitiesListOptions(APPLIES_TO_ENTITIES)}
                            onUpdate={(values) => handleSelectChange('entityTypes', values)}
                            placeholder="Select Entity Types"
                            isMultiSelect
                        />
                    </Form.Item>
                </RowContainer>

                <AdvancedOptions />
            </Form>
        </StyledDrawer>
    );
};

export default StructuredPropsDrawer;
