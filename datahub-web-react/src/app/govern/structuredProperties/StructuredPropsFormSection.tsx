import { Icon, SimpleSelect, Text, Tooltip } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, SearchResult } from '@src/types.generated';
import { Form, FormInstance } from 'antd';
import React from 'react';
import AllowedValuesField from './AllowedValuesField';
import RequiredAsterisk from './RequiredAsterisk';
import { FieldLabel, FlexContainer, RowContainer, SubTextContainer } from './styledComponents';
import useStructuredProp from './useStructuredProp';
import {
    APPLIES_TO_ENTITIES,
    isEntityTypeSelected,
    PropValueField,
    SEARCHABLE_ENTITY_TYPES,
    StructuredProp,
} from './utils';

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

const StructuredPropsFormSection = ({
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
    const {
        handleSelectChange,
        handleSelectUpdateChange,
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

    return (
        <>
            {!(isEditMode && !allowedValues) && (
                <AllowedValuesField
                    selectedValueType={selectedValueType}
                    allowedValues={allowedValues}
                    valueField={valueField}
                    setShowAllowedValuesDrawer={setShowAllowedValuesDrawer}
                />
            )}
            {isEntityTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <FlexContainer>
                            Allowed Entity Types
                            <Tooltip
                                title="Choose the types of entities that are allowed as values for this property"
                                showArrow={false}
                            >
                                <Icon icon="Info" color="violet" size="lg" />
                            </Tooltip>
                        </FlexContainer>
                        {isEditMode && (
                            <SubTextContainer>
                                <Text size="sm" weight="medium">
                                    <Tooltip
                                        title="Once a property is created, entity types cannot be removed"
                                        showArrow={false}
                                    >
                                        (Add-only)
                                    </Tooltip>
                                </Text>
                            </SubTextContainer>
                        )}
                    </FieldLabel>
                    <Tooltip
                        title={
                            !formValues?.typeQualifier?.allowedTypes?.length &&
                            'Any entity type will be accepted as a value'
                        }
                        showArrow={false}
                    >
                        <Form.Item name={['typeQualifier', 'allowedTypes']}>
                            <SimpleSelect
                                options={getEntitiesListOptions(SEARCHABLE_ENTITY_TYPES)}
                                onUpdate={(values) =>
                                    isEditMode
                                        ? handleSelectUpdateChange(['typeQualifier', 'allowedTypes'], values)
                                        : handleSelectChange(['typeQualifier', 'allowedTypes'], values)
                                }
                                placeholder="Any"
                                isMultiSelect
                                values={formValues?.typeQualifier?.allowedTypes}
                                disabledValues={disabledTypeQualifierValues}
                                width="full"
                            />
                        </Form.Item>
                    </Tooltip>
                </RowContainer>
            )}
            <RowContainer>
                <FieldLabel>
                    <FlexContainer>
                        Applies to
                        <RequiredAsterisk />
                        <Tooltip
                            title="Select the types of entities that this property can be added to"
                            showArrow={false}
                        >
                            <Icon icon="Info" color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                    {isEditMode && (
                        <SubTextContainer>
                            <Text size="sm" weight="medium">
                                <Tooltip
                                    title="Once a property is created entity types cannot be removed"
                                    showArrow={false}
                                >
                                    (Add-only)
                                </Tooltip>
                            </Text>
                        </SubTextContainer>
                    )}
                </FieldLabel>

                <Form.Item
                    name="entityTypes"
                    rules={[
                        {
                            required: true,
                            message: 'Please select asset types this applies to',
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
                        width="full"
                        showSelectAll
                        selectAllLabel="All Asset Types"
                        data-testid="structured-props-select-input-applies-to"
                        optionListTestId="applies-to-options-list"
                    />
                </Form.Item>
            </RowContainer>
        </>
    );
};

export default StructuredPropsFormSection;
