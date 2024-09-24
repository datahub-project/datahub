import { Icon, Input, SimpleSelect, Text, TextArea } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import { Form, FormInstance, Tooltip } from 'antd';
import React, { useEffect, useState } from 'react';
import AdvancedOptions from './AdvancedOptions';
import AllowedValuesModal from './AllowedValuesModal';
import {
    FieldLabel,
    ItemsContainer,
    RowContainer,
    StyledIcon,
    SubTextContainer,
    ValueListContainer,
    ValuesList,
    VerticalDivider,
} from './styledComponents';
import useStructuredProp from './useStructuredProp';
import {
    APPLIES_TO_ENTITIES,
    getDisplayName,
    getStringOrNumberValueField,
    getValueType,
    isEntityTypeSelected,
    isStringOrNumberTypeSelected,
    PropValueField,
    SEARCHABLE_ENTITY_TYPES,
    StructuredProp,
    valueTypes,
} from './utils';

interface Props {
    selectedProperty?: SearchResult;
    form: FormInstance;
    formValues: StructuredProp | undefined;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    isEditMode: boolean;
    selectedValueType: string;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
    allowedValues: AllowedValue[] | undefined;
    setAllowedValues: React.Dispatch<React.SetStateAction<AllowedValue[] | undefined>>;
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
    allowedValues,
    setAllowedValues,
}: Props) => {
    const {
        handleSelectChange,
        handleSelectUpdateChange,
        handleTypeUpdate,
        getEntitiesListOptions,
        disabledEntityTypeValues,
        disabledTypeQualifierValues,
    } = useStructuredProp({
        selectedProperty,
        form,
        setFormValues,
        setCardinality,
        setSelectedValueType,
    });

    const [showAllowedValuesModal, setShowAllowedValuesModal] = useState<boolean>(false);
    const [valueField, setValueField] = useState<PropValueField>('stringValue');

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

    useEffect(() => {
        const entity = selectedProperty?.entity as StructuredPropertyEntity;
        const field = getStringOrNumberValueField(selectedValueType);
        setValueField(field);
        const allowedList = entity?.definition?.allowedValues?.map((item) => {
            return {
                [field]: item.value[field],
                description: item.description,
            } as AllowedValue;
        });
        setAllowedValues(allowedList);
    }, [selectedProperty, selectedValueType, setAllowedValues]);

    useEffect(() => {
        form.setFieldValue('allowedValues', allowedValues);
    }, [allowedValues, form]);

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
            {isStringOrNumberTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <span>Allowed Value List </span>
                        {isEditMode && (
                            <SubTextContainer>
                                <Text size="sm" weight="medium">
                                    (Add-only)
                                </Text>
                                <Tooltip title="Once a structured property is created, you can only add new allowed values to preserve backwards compatibility">
                                    <Icon icon="Info" color="violet" size="lg" />
                                </Tooltip>
                            </SubTextContainer>
                        )}
                    </FieldLabel>
                    {allowedValues && allowedValues.length > 0 ? (
                        <ItemsContainer>
                            <ValuesList>
                                {allowedValues.map((val, index) => {
                                    return (
                                        <>
                                            <Text>{val[valueField]}</Text>
                                            {index < allowedValues.length - 1 && <VerticalDivider type="vertical" />}
                                        </>
                                    );
                                })}
                            </ValuesList>
                            <StyledIcon
                                icon="ChevronRight"
                                color="gray"
                                onClick={() => setShowAllowedValuesModal(true)}
                            />
                        </ItemsContainer>
                    ) : (
                        <ValueListContainer>
                            <Tooltip title="Add new allowed values">
                                <Icon icon="Add" color="gray" onClick={() => setShowAllowedValuesModal(true)} />
                            </Tooltip>
                        </ValueListContainer>
                    )}
                </RowContainer>
            )}
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
                            disabledValues={disabledTypeQualifierValues}
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
                        disabledValues={disabledEntityTypeValues}
                    />
                </Form.Item>
            </RowContainer>
            <AdvancedOptions isEditMode={isEditMode} />
            <AllowedValuesModal
                isOpen={showAllowedValuesModal}
                showAllowedValuesModal={showAllowedValuesModal}
                setShowAllowedValuesModal={setShowAllowedValuesModal}
                propType={valueField}
                allowedValues={allowedValues}
                setAllowedValues={setAllowedValues}
                isEditMode={isEditMode}
                noOfExistingValues={
                    (selectedProperty?.entity as StructuredPropertyEntity)?.definition?.allowedValues?.length || 0
                }
            />
        </Form>
    );
};

export default StructuredPropsForm;
