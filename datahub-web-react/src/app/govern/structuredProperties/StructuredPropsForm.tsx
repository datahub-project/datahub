import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import AdvancedOptions from '@app/govern/structuredProperties/AdvancedOptions';
import DisplayPreferences from '@app/govern/structuredProperties/DisplayPreferences';
import StructuredPropsFormSection from '@app/govern/structuredProperties/StructuredPropsFormSection';
import { FieldError, FormContainer } from '@app/govern/structuredProperties/styledComponents';
import useStructuredProp from '@app/govern/structuredProperties/useStructuredProp';
import {
    AllowedValueRow,
    PropValueField,
    StructuredProp,
    StructuredPropertyFormErrors,
    valueTypes,
} from '@app/govern/structuredProperties/utils';
import { Input, SimpleSelect, TextArea } from '@src/alchemy-components';
import { AllowedValueInput, PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';

type Props = {
    selectedProperty: StructuredPropertyEntity | undefined;
    isReadOnly: boolean;
    formValues: StructuredProp | undefined;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setFieldValue: (field: keyof StructuredProp, value: StructuredProp[keyof StructuredProp]) => void;
    errors: StructuredPropertyFormErrors;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    isEditMode: boolean;
    selectedValueType: string;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
    /** The allowed values the property was saved with; absent when it has none. */
    savedAllowedValues: AllowedValueRow[] | undefined;
    allowedValueRows: AllowedValueRow[];
    /** Unsaved rows with a value, in list order. */
    liveAllowedValues: AllowedValueInput[];
    addAllowedValueRow: () => void;
    updateAllowedValueRow: (rowId: string, patch: Partial<AllowedValueRow>) => void;
    removeAllowedValueRow: (rowId: string) => void;
    moveAllowedValueRow: (from: number, to: number) => void;
    valueField: PropValueField;
    badgeProperty?: StructuredPropertyEntity;
    /** Fires on user edits only, not on programmatic value changes. */
    onValuesChange?: () => void;
};

const StructuredPropsForm = ({
    selectedProperty,
    isReadOnly,
    formValues,
    setFormValues,
    setFieldValue,
    errors,
    isEditMode,
    setCardinality,
    selectedValueType,
    setSelectedValueType,
    savedAllowedValues,
    allowedValueRows,
    liveAllowedValues,
    addAllowedValueRow,
    updateAllowedValueRow,
    removeAllowedValueRow,
    moveAllowedValueRow,
    valueField,
    badgeProperty,
    onValuesChange,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tl } = useTranslation('common.labels');
    const structuredPropActions = useStructuredProp({
        selectedProperty,
        setFormValues,
        setCardinality,
        setSelectedValueType,
    });
    const { handleTypeUpdate, handleDisplaySettingChange } = structuredPropActions;

    return (
        <FormContainer>
            <Input
                label={tl('name')}
                placeholder={t('create.namePlaceholder')}
                isRequired
                value={formValues?.displayName ?? ''}
                setValue={(value) => setFieldValue('displayName', value)}
                error={errors.displayName}
                isDisabled={isReadOnly}
                data-testid="structured-props-input-name"
            />
            <TextArea
                label={tl('description')}
                placeholder={t('allowedValues.descriptionPlaceholder')}
                value={formValues?.description ?? ''}
                onChange={(e) => setFieldValue('description', e.target.value)}
                isDisabled={isReadOnly}
                data-testid="structured-props-input-description"
            />
            <Tooltip title={isEditMode && t('create.propertyTypeDisabledTooltip')} showArrow={false}>
                <div>
                    <SimpleSelect
                        label={t('create.propertyType')}
                        isRequired
                        onUpdate={(values) => {
                            handleTypeUpdate(values[0]);
                            onValuesChange?.();
                        }}
                        placeholder={t('create.propertyTypePlaceholder')}
                        options={valueTypes}
                        values={formValues?.valueType ? [formValues.valueType] : undefined}
                        isDisabled={isEditMode || isReadOnly}
                        showDescriptions
                        data-testid="structured-props-select-input-type"
                        optionListTestId="structured-props-property-type-options-list"
                        width="full"
                    />
                    {errors.valueType && <FieldError>{errors.valueType}</FieldError>}
                </div>
            </Tooltip>

            <StructuredPropsFormSection
                selectedProperty={selectedProperty}
                isReadOnly={isReadOnly}
                formValues={formValues}
                errors={errors}
                isEditMode={isEditMode}
                selectedValueType={selectedValueType}
                selectionActions={structuredPropActions}
                savedAllowedValues={savedAllowedValues}
                allowedValueRows={allowedValueRows}
                addAllowedValueRow={addAllowedValueRow}
                updateAllowedValueRow={updateAllowedValueRow}
                removeAllowedValueRow={removeAllowedValueRow}
                moveAllowedValueRow={moveAllowedValueRow}
                valueField={valueField}
                onValuesChange={onValuesChange}
            />
            <DisplayPreferences
                formValues={formValues}
                handleDisplaySettingChange={(field, value) => {
                    handleDisplaySettingChange(field, value);
                    onValuesChange?.();
                }}
                selectedValueType={selectedValueType}
                badgeProperty={badgeProperty}
                // Allowed values are edited inline, so display preferences (e.g. the asset badge
                // toggle, which requires a bounded value set) must react to unsaved edits rather
                // than the saved definition.
                allowedValues={liveAllowedValues}
                isReadOnly={isReadOnly}
            />
            <AdvancedOptions
                isEditMode={isEditMode}
                isReadOnly={isReadOnly}
                qualifiedName={formValues?.qualifiedName}
                setQualifiedName={(value) => setFieldValue('qualifiedName', value)}
                error={errors.qualifiedName}
            />
        </FormContainer>
    );
};

export default StructuredPropsForm;
