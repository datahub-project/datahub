import { Icon, SimpleSelect, Text } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, SearchResult } from '@src/types.generated';
import { Form, FormInstance, Tooltip } from 'antd';
import React from 'react';
import AllowedValuesField from './AllowedValuesField';
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
    setAllowedValues: React.Dispatch<React.SetStateAction<AllowedValue[] | undefined>>;
    valueField: PropValueField;
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
    setAllowedValues,
    valueField,
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
                    selectedProperty={selectedProperty}
                    isEditMode={isEditMode}
                    selectedValueType={selectedValueType}
                    allowedValues={allowedValues}
                    setAllowedValues={setAllowedValues}
                    valueField={valueField}
                />
            )}
            {isEntityTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <FlexContainer>
                            Allowed Asset Types
                            <Tooltip title="Optionally choose which asset types are valid values that can be set on an asset with this structured property. For example, choosing 'Person' and 'Group' will only allow users to select users or groups as values on an asset for this property.">
                                <Icon icon="Info" color="violet" size="lg" />
                            </Tooltip>
                        </FlexContainer>
                        {isEditMode && (
                            <SubTextContainer>
                                <Text size="sm" weight="medium">
                                    (Add-only)
                                </Text>
                                <Tooltip title="Once a structured property is created, you can only add new allowed asset types to preserve backwards compatibility">
                                    <Icon icon="Info" color="violet" size="md" />
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
                            placeholder="Select Allowed Asset Types"
                            isMultiSelect
                            values={formValues?.typeQualifier?.allowedTypes}
                            disabledValues={disabledTypeQualifierValues}
                        />
                    </Form.Item>
                </RowContainer>
            )}
            <RowContainer>
                <FieldLabel>
                    <FlexContainer>
                        Applies to
                        <Tooltip title="Select the asset types that this structured property can be applied to.">
                            <Icon icon="Info" color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                    {isEditMode && (
                        <SubTextContainer>
                            <Text size="sm" weight="medium">
                                (Add-only)
                            </Text>
                            <Tooltip title="Once a structured property is created, you can only add to the applies to list to preserve backwards compatibility">
                                <Icon icon="Info" color="violet" size="md" />
                            </Tooltip>
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
                        placeholder="Select Asset Types"
                        isMultiSelect
                        values={formValues?.entityTypes ? formValues?.entityTypes : undefined}
                        disabledValues={disabledEntityTypeValues}
                    />
                </Form.Item>
            </RowContainer>
        </>
    );
};

export default StructuredPropsFormSection;
