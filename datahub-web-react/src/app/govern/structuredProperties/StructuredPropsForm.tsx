import { Icon, Input, SimpleSelect, TextArea } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import { Form, FormInstance } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import AdvancedOptions from './AdvancedOptions';
import RequiredAsterisk from './RequiredAsterisk';
import DisplayPreferences from './DisplayPreferences';
import StructuredPropsFormSection from './StructuredPropsFormSection';
import { FieldLabel, FlexContainer, GridFormItem, RowContainer } from './styledComponents';
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
    refetchProperties: () => void;
    badgeProperty?: StructuredPropertyEntity;
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
    refetchProperties,
    badgeProperty,
}: Props) => {
    const { handleTypeUpdate, handleDisplaySettingChange } = useStructuredProp({
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
                <Input label="Name" placeholder="Provide a name" isRequired data-testid="structured-props-input-name" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea
                    label="Description"
                    placeholder="Provide a description"
                    data-testid="structured-props-input-description"
                />
            </Form.Item>
            <RowContainer>
                <FieldLabel>
                    <FlexContainer>
                        Property Type
                        <RequiredAsterisk />
                        <Tooltip title="The allowed value type of the property" showArrow={false}>
                            <Icon icon="Info" color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                </FieldLabel>

                <Tooltip
                    title={isEditMode && 'Once a property is created, its type cannot be changed'}
                    showArrow={false}
                >
                    <GridFormItem
                        name="valueType"
                        rules={[
                            {
                                required: true,
                                message: 'Please select the property type',
                            },
                        ]}
                    >
                        <SimpleSelect
                            onUpdate={(values: any) => {
                                handleTypeUpdate(values[0]);
                            }}
                            placeholder="Select Property Type"
                            options={valueTypes}
                            values={formValues?.valueType ? [formValues?.valueType] : undefined}
                            isDisabled={isEditMode}
                            showDescriptions
                            data-testid="structured-props-select-input-type"
                            optionListTestId="structured-props-property-type-options-list"
                        />
                    </GridFormItem>
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
            <DisplayPreferences
                formValues={formValues}
                handleDisplaySettingChange={handleDisplaySettingChange}
                selectedValueType={selectedValueType}
                refetchProperties={refetchProperties}
                badgeProperty={badgeProperty}
                allowedValues={allowedValues}
            />
            <AdvancedOptions isEditMode={isEditMode} />
        </Form>
    );
};

export default StructuredPropsForm;
