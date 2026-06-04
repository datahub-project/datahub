import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import AllowedValuesField from '@app/govern/structuredProperties/AllowedValuesField';
import RequiredAsterisk from '@app/govern/structuredProperties/RequiredAsterisk';
import {
    FieldLabel,
    FlexContainer,
    RowContainer,
    SubTextContainer,
} from '@app/govern/structuredProperties/styledComponents';
import useStructuredProp from '@app/govern/structuredProperties/useStructuredProp';
import {
    APPLIES_TO_ENTITIES,
    PropValueField,
    SEARCHABLE_ENTITY_TYPES,
    StructuredProp,
    isEntityTypeSelected,
} from '@app/govern/structuredProperties/utils';
import { Icon, SimpleSelect, Text, Tooltip } from '@src/alchemy-components';
import { AllowedValue, PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';

const ALLOWED_TYPES_FIELD_PATH = ['typeQualifier', 'allowedTypes'];
const ENTITY_TYPES_FIELD = 'entityTypes';

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
    const { t } = useTranslation('governance.structured-properties');
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
                            {t('allowedEntityTypes.title')}
                            <Tooltip title={t('allowedEntityTypes.tooltip')} showArrow={false}>
                                <Icon icon={Info} color="violet" size="lg" />
                            </Tooltip>
                        </FlexContainer>
                        {isEditMode && (
                            <SubTextContainer>
                                <Text size="sm" weight="medium">
                                    <Tooltip title={t('addOnlyTooltip')} showArrow={false}>
                                        {t('addOnly')}
                                    </Tooltip>
                                </Text>
                            </SubTextContainer>
                        )}
                    </FieldLabel>
                    <Tooltip
                        title={!formValues?.typeQualifier?.allowedTypes?.length && t('allowedEntityTypes.anyTooltip')}
                        showArrow={false}
                    >
                        <Form.Item name={['typeQualifier', 'allowedTypes']}>
                            <SimpleSelect
                                options={getEntitiesListOptions(SEARCHABLE_ENTITY_TYPES)}
                                onUpdate={(values) =>
                                    isEditMode
                                        ? handleSelectUpdateChange(ALLOWED_TYPES_FIELD_PATH, values)
                                        : handleSelectChange(ALLOWED_TYPES_FIELD_PATH, values)
                                }
                                placeholder={t('allowedEntityTypes.anyPlaceholder')}
                                isMultiSelect
                                values={formValues?.typeQualifier?.allowedTypes}
                                disabledValues={disabledTypeQualifierValues}
                                width="full"
                                isDisabled={isEditMode ? !formValues?.typeQualifier?.allowedTypes?.length : false}
                            />
                        </Form.Item>
                    </Tooltip>
                </RowContainer>
            )}
            <RowContainer>
                <FieldLabel>
                    <FlexContainer>
                        {t('appliesTo.title')}
                        <RequiredAsterisk />
                        <Tooltip title={t('appliesTo.tooltip')} showArrow={false}>
                            <Icon icon={Info} color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                    {isEditMode && (
                        <SubTextContainer>
                            <Text size="sm" weight="medium">
                                <Tooltip title={t('addOnlyTooltip')} showArrow={false}>
                                    {t('addOnly')}
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
                            message: t('appliesTo.error'),
                        },
                    ]}
                >
                    <SimpleSelect
                        options={getEntitiesListOptions(APPLIES_TO_ENTITIES)}
                        onUpdate={(values) =>
                            isEditMode
                                ? handleSelectUpdateChange(ENTITY_TYPES_FIELD, values)
                                : handleSelectChange(ENTITY_TYPES_FIELD, values)
                        }
                        placeholder={t('appliesTo.placeholder')}
                        isMultiSelect
                        values={formValues?.entityTypes ? formValues?.entityTypes : undefined}
                        disabledValues={disabledEntityTypeValues}
                        width="full"
                        showSelectAll
                        selectAllLabel={t('appliesTo.allAssetTypes')}
                        data-testid="structured-props-select-input-applies-to"
                        optionListTestId="applies-to-options-list"
                    />
                </Form.Item>
            </RowContainer>
        </>
    );
};

export default StructuredPropsFormSection;
