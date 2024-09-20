import { Icon, Input, SimpleSelect, Text, TextArea } from '@src/alchemy-components';
import { PropertyCardinality, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import { Form, FormInstance, Tooltip } from 'antd';
import React, { useEffect } from 'react';
import AdvancedOptions from './AdvancedOptions';
import { FieldLabel, RowContainer, SubTextContainer } from './styledComponents';
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
    selectedProperty?: SearchResult;
    form: FormInstance;
    formValues?: StructuredProp;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    isEditMode: boolean;
    selectedValueType: string;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
}

const StructuredPropsForm = ({
    selectedProperty,
    form,
    formValues,
    setFormValues,
    isEditMode,
    setCardinality,
    selectedValueType,
    setSelectedValueType,
}: Props) => {
    const {
        handleSelectChange,
        handleSelectUpdateChange,
        handleTypeUpdate,
        getEntitiesListOptions,
        getDisabledEntityTypeValues,
        getDisabledTypeQualifierValues,
    } = useStructuredProp({
        selectedProperty,
        form,
        setFormValues,
        setCardinality,
        setSelectedValueType,
    });

    useEffect(() => {
        if (selectedProperty) {
            const entity = selectedProperty.entity as StructuredPropertyEntity;
            const typeValue = getValueType(
                entity.definition.valueType.urn,
                entity.definition.cardinality || PropertyCardinality.Single,
            );
            const values: StructuredProp = {
                displayName: getDisplayName(entity),
                description: entity.definition.description,
                qualifiedName: entity.definition.qualifiedName,
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
        } else {
            setFormValues(undefined);
            form.resetFields();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedProperty, form]);

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
                <Tooltip
                    title={
                        isEditMode &&
                        'Changing type is disabled once a structured property is created to preserve backwards compatibility'
                    }
                    showArrow={false}
                >
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
                </Tooltip>
            </RowContainer>
            {isEntityTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <span>Allowed Entity Types </span>
                        {isEditMode && (
                            <SubTextContainer>
                                <Text size="sm" weight="medium">
                                    (Add-only)
                                </Text>
                                <Tooltip title="Once a structured property is created, you can only add new allowed entity types to preserve backwards compatibility">
                                    <Icon icon="Info" color="violet" size="lg" />
                                </Tooltip>
                            </SubTextContainer>
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
                            disabledValues={getDisabledTypeQualifierValues()}
                        />
                    </Form.Item>
                </RowContainer>
            )}
            <RowContainer>
                <FieldLabel>
                    <span>Applies to</span>
                    {isEditMode && (
                        <SubTextContainer>
                            <Text size="sm" weight="medium">
                                (Add-only)
                            </Text>
                            <Tooltip title="Once a structured property is created, you can only add to the applies to list to preserve backwards compatibility">
                                <Icon icon="Info" color="violet" size="lg" />
                            </Tooltip>
                        </SubTextContainer>
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
                        disabledValues={getDisabledEntityTypeValues()}
                    />
                </Form.Item>
            </RowContainer>
            <AdvancedOptions isEditMode={isEditMode} />
        </Form>
    );
};

export default StructuredPropsForm;
