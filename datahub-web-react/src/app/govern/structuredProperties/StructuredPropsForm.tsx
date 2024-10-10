import { Icon, Input, Text, TextArea } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, SearchResult } from '@src/types.generated';
import { Form, FormInstance, Select, Tooltip } from 'antd';
import React from 'react';
import AdvancedOptions from './AdvancedOptions';
import StructuredPropsFormSection from './StructuredPropsFormSection';
import {
    CustomDropdown,
    FieldLabel,
    FlexContainer,
    RowContainer,
    SelectOptionContainer,
    StyledSelect,
} from './styledComponents';
import useStructuredProp from './useStructuredProp';
import { PropValueField, StructuredProp, valueTypes } from './utils';

interface Props {
    selectedProperty: SearchResult | undefined;
    form: FormInstance;
    formValues: StructuredProp | undefined;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    isEditMode: boolean;
    selectedValueType: string;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
    allowedValues: AllowedValue[] | undefined;
    valueField: PropValueField;
    setShowAllowedValuesDrawer: React.Dispatch<React.SetStateAction<boolean>>;
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
    valueField,
    setShowAllowedValuesDrawer,
}: Props) => {
    const { handleTypeUpdate, handleFilterStatusChange } = useStructuredProp({
        selectedProperty,
        form,
        setFormValues,
        setCardinality,
        setSelectedValueType,
    });

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
                <Input label="Name" placeholder="Provide a name" isRequired />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description" placeholder="Provide a description" />
            </Form.Item>
            <RowContainer>
                <FieldLabel>
                    <FlexContainer>
                        Property Type
                        <Text color="red" weight="bold">
                            *
                        </Text>
                        <Tooltip title="The allowed value type of the property" showArrow={false}>
                            <Icon icon="Info" color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                </FieldLabel>

                <Tooltip
                    title={isEditMode && 'Once a property is created, its type cannot be changed'}
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
                        <StyledSelect
                            onChange={(value: any) => {
                                handleTypeUpdate(value);
                            }}
                            placeholder="Select Property Type"
                            disabled={isEditMode}
                            dropdownRender={(menu) => <CustomDropdown>{menu}</CustomDropdown>}
                        >
                            {valueTypes.map((valType) => {
                                return (
                                    <Select.Option key={valType.value} value={valType.value}>
                                        <SelectOptionContainer>
                                            <Text color="gray" weight="medium" size="md">
                                                {valType.label}
                                            </Text>
                                            <Text color="gray" weight="normal" size="sm">
                                                {valType.description}
                                            </Text>
                                        </SelectOptionContainer>
                                    </Select.Option>
                                );
                            })}
                        </StyledSelect>
                    </Form.Item>
                </Tooltip>
            </RowContainer>

            <StructuredPropsFormSection
                selectedProperty={selectedProperty}
                form={form}
                formValues={formValues}
                setFormValues={setFormValues}
                isEditMode={isEditMode}
                setCardinality={setCardinality}
                selectedValueType={selectedValueType}
                setSelectedValueType={setSelectedValueType}
                allowedValues={allowedValues}
                valueField={valueField}
                setShowAllowedValuesDrawer={setShowAllowedValuesDrawer}
            />
            <AdvancedOptions
                isEditMode={isEditMode}
                handleFilterStatusChange={handleFilterStatusChange}
                formValues={formValues}
            />
        </Form>
    );
};

export default StructuredPropsForm;
