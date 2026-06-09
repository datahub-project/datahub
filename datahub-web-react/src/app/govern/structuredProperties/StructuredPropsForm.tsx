import { Tooltip } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import AdvancedOptions from '@app/govern/structuredProperties/AdvancedOptions';
import DisplayPreferences from '@app/govern/structuredProperties/DisplayPreferences';
import RequiredAsterisk from '@app/govern/structuredProperties/RequiredAsterisk';
import StructuredPropsFormSection from '@app/govern/structuredProperties/StructuredPropsFormSection';
import {
    FieldLabel,
    FlexContainer,
    GridFormItem,
    RowContainer,
} from '@app/govern/structuredProperties/styledComponents';
import useStructuredProp from '@app/govern/structuredProperties/useStructuredProp';
import { PropValueField, StructuredProp, valueTypes } from '@app/govern/structuredProperties/utils';
import { Icon, Input, SimpleSelect, TextArea } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    selectedProperty: StructuredPropertyEntity | undefined;
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
    const { t } = useTranslation('governance.structured-properties');
    const { t: tl } = useTranslation('common.labels');
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
                        message: t('create.nameError'),
                    },
                ]}
            >
                <Input
                    label={tl('name')}
                    placeholder={t('create.namePlaceholder')}
                    isRequired
                    data-testid="structured-props-input-name"
                />
            </Form.Item>
            <Form.Item name="description">
                <TextArea
                    label={tl('description')}
                    placeholder={t('allowedValues.descriptionPlaceholder')}
                    data-testid="structured-props-input-description"
                />
            </Form.Item>
            <RowContainer>
                <FieldLabel>
                    <FlexContainer>
                        {t('create.propertyType')}
                        <RequiredAsterisk />
                        <Tooltip title={t('create.propertyTypeTooltip')} showArrow={false}>
                            <Icon icon={Info} color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                </FieldLabel>

                <Tooltip title={isEditMode && t('create.propertyTypeDisabledTooltip')} showArrow={false}>
                    <GridFormItem
                        name="valueType"
                        rules={[
                            {
                                required: true,
                                message: t('create.propertyTypeError'),
                            },
                        ]}
                    >
                        <SimpleSelect
                            onUpdate={(values: any) => {
                                handleTypeUpdate(values[0]);
                            }}
                            placeholder={t('create.propertyTypePlaceholder')}
                            options={valueTypes}
                            values={formValues?.valueType ? [formValues?.valueType] : undefined}
                            isDisabled={isEditMode}
                            showDescriptions
                            data-testid="structured-props-select-input-type"
                            optionListTestId="structured-props-property-type-options-list"
                            width="full"
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
