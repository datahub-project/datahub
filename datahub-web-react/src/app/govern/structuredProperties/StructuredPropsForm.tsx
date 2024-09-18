import { Input, SimpleSelect, Text, TextArea } from '@src/alchemy-components';
import { PropertyCardinality, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import { Form, FormInstance } from 'antd';
import React, { useEffect } from 'react';
import AdvancedOptions from './AdvancedOptions';
import { FieldLabel, RowContainer } from './styledComponents';
import useStructuredProp from './useStructuredProp';
import {
    APPLIES_TO_ENTITIES,
    getDisplayName,
    getValueType,
    isEntityTypeSelected,
    SEARCHABLE_ENTITY_TYPES,
    StructuredProp,
    valueTypes,
} from './utils';

interface Props {
    currentProperty?: SearchResult;
    form: FormInstance;
    formValues?: StructuredProp;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    isEditMode: boolean;
    selectedValueType: string;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
}

const StructuredPropsForm = ({
    currentProperty,
    form,
    formValues,
    setFormValues,
    isEditMode,
    setCardinality,
    selectedValueType,
    setSelectedValueType,
}: Props) => {
    const { handleSelectChange, handleSelectUpdateChange, handleTypeUpdate, getEntitiesListOptions } =
        useStructuredProp({
            currentProperty,
            form,
            setFormValues,
            setCardinality,
            setSelectedValueType,
        });

    useEffect(() => {
        if (currentProperty) {
            const entity = currentProperty.entity as StructuredPropertyEntity;
            const typeValue = getValueType(
                entity.definition.valueType.urn,
                entity.definition.cardinality || PropertyCardinality.Single,
            );
            const values = {
                displayName: getDisplayName(entity),
                description: entity.definition.description,
                qualifiedName: entity.definition.qualifiedName,
                id: entity.urn,
                valueType: typeValue,
                entityTypes: entity.definition.entityTypes.map((entityType) => entityType.urn),
                typeQualifier: {
                    allowedTypes: entity.definition.typeQualifier?.allowedTypes?.map((entityType) => entityType.urn),
                },
                immutable: entity.definition.immutable,
            };
            setFormValues(values);
            if (typeValue) handleTypeUpdate([typeValue]);
            form.setFieldsValue(values);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [currentProperty, form]);

    return (
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
                        onUpdate={(values) => {
                            handleTypeUpdate(values);
                        }}
                        placeholder="Select Property Type"
                        values={formValues?.valueType ? [formValues?.valueType] : undefined}
                        isDisabled={isEditMode}
                    />
                </Form.Item>
            </RowContainer>
            {isEntityTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <span>Allowed Entity Types </span>
                        {isEditMode && (
                            <Text size="sm" color="gray">
                                (Append-only)
                            </Text>
                        )}
                    </FieldLabel>
                    <Form.Item name={['typeQualifier', 'allowedTypes']}>
                        <SimpleSelect
                            options={getEntitiesListOptions(SEARCHABLE_ENTITY_TYPES)}
                            onUpdate={(values) =>
                                isEditMode
                                    ? handleSelectUpdateChange(['typeQualifier', 'allowedTypes'], values)
                                    : handleSelectChange(['typeQualifier', 'allowedTypes'], values)
                            }
                            placeholder="Select Allowed Entity Types"
                            isMultiSelect
                            values={formValues?.typeQualifier?.allowedTypes}
                        />
                    </Form.Item>
                </RowContainer>
            )}
            <RowContainer>
                <FieldLabel>
                    <span>Applies to</span>
                    {isEditMode && (
                        <Text size="sm" color="gray">
                            (Append-only)
                        </Text>
                    )}
                </FieldLabel>

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
                        onUpdate={(values) =>
                            isEditMode
                                ? handleSelectUpdateChange('entityTypes', values)
                                : handleSelectChange('entityTypes', values)
                        }
                        placeholder="Select Entity Types"
                        isMultiSelect
                        values={formValues?.entityTypes ? formValues?.entityTypes : undefined}
                    />
                </Form.Item>
            </RowContainer>
            <AdvancedOptions isEditMode={isEditMode} />
        </Form>
    );
};

export default StructuredPropsForm;
