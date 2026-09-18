import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AllowedValuesField from '@app/govern/structuredProperties/AllowedValuesField';
import { FieldError } from '@app/govern/structuredProperties/styledComponents';
import useAvailablePlatforms, { PlatformOption } from '@app/govern/structuredProperties/useAvailablePlatforms';
import type { StructuredPropActions } from '@app/govern/structuredProperties/useStructuredProp';
import {
    APPLIES_TO_ENTITIES,
    AllowedValueRow,
    PropValueField,
    SEARCHABLE_ENTITY_TYPES,
    StructuredProp,
    StructuredPropertyFormErrors,
    isEntityTypeSelected,
} from '@app/govern/structuredProperties/utils';
import { SimpleSelect, Tooltip } from '@src/alchemy-components';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { StructuredPropertyEntity } from '@src/types.generated';

const PlatformOptionLabel = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ALLOWED_TYPES_FIELD_PATH = ['typeQualifier', 'allowedTypes'];
const ENTITY_TYPES_FIELD = 'entityTypes';
const ALLOWED_PLATFORMS_FIELD = 'allowedPlatforms';

type SelectionActions = Pick<
    StructuredPropActions,
    | 'handleSelectChange'
    | 'handleSelectUpdateChange'
    | 'getEntitiesListOptions'
    | 'disabledEntityTypeValues'
    | 'disabledAllowedPlatformValues'
    | 'disabledTypeQualifierValues'
>;

type Props = {
    selectedProperty: StructuredPropertyEntity | undefined;
    isReadOnly: boolean;
    formValues: StructuredProp | undefined;
    errors: StructuredPropertyFormErrors;
    isEditMode: boolean;
    selectedValueType: string;
    selectionActions: SelectionActions;
    savedAllowedValues: AllowedValueRow[] | undefined;
    allowedValueRows: AllowedValueRow[];
    addAllowedValueRow: () => void;
    updateAllowedValueRow: (rowId: string, patch: Partial<AllowedValueRow>) => void;
    removeAllowedValueRow: (rowId: string) => void;
    moveAllowedValueRow: (from: number, to: number) => void;
    valueField: PropValueField;
    onValuesChange?: () => void;
};

const StructuredPropsFormSection = ({
    selectedProperty,
    isReadOnly,
    formValues,
    errors,
    isEditMode,
    selectedValueType,
    selectionActions,
    savedAllowedValues,
    allowedValueRows,
    addAllowedValueRow,
    updateAllowedValueRow,
    removeAllowedValueRow,
    moveAllowedValueRow,
    valueField,
    onValuesChange,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const platformOptions = useAvailablePlatforms();

    const {
        handleSelectChange,
        handleSelectUpdateChange,
        getEntitiesListOptions,
        disabledEntityTypeValues,
        disabledAllowedPlatformValues,
        disabledTypeQualifierValues,
    } = selectionActions;

    const handleUpdate = (field: string | string[], values: string[]) => {
        if (isEditMode) handleSelectUpdateChange(field, values);
        else handleSelectChange(field, values);
        onValuesChange?.();
    };

    return (
        <>
            {!(isEditMode && !savedAllowedValues) && (
                <AllowedValuesField
                    selectedValueType={selectedValueType}
                    valueField={valueField}
                    isReadOnly={isReadOnly}
                    rows={allowedValueRows}
                    errors={errors.allowedValues}
                    addRow={addAllowedValueRow}
                    updateRow={updateAllowedValueRow}
                    removeRow={removeAllowedValueRow}
                    moveRow={moveAllowedValueRow}
                />
            )}
            {isEntityTypeSelected(selectedValueType) && (
                <Tooltip
                    title={!formValues?.typeQualifier?.allowedTypes?.length && t('allowedEntityTypes.anyTooltip')}
                    showArrow={false}
                >
                    <div>
                        <SimpleSelect
                            label={t('allowedEntityTypes.title')}
                            options={getEntitiesListOptions(SEARCHABLE_ENTITY_TYPES)}
                            onUpdate={(values) => handleUpdate(ALLOWED_TYPES_FIELD_PATH, values)}
                            placeholder={t('allowedEntityTypes.anyPlaceholder')}
                            isMultiSelect
                            values={formValues?.typeQualifier?.allowedTypes}
                            disabledValues={disabledTypeQualifierValues}
                            width="full"
                            isDisabled={
                                isReadOnly || (isEditMode ? !formValues?.typeQualifier?.allowedTypes?.length : false)
                            }
                        />
                    </div>
                </Tooltip>
            )}
            <div>
                <SimpleSelect
                    label={t('appliesTo.title')}
                    isRequired
                    options={getEntitiesListOptions(APPLIES_TO_ENTITIES)}
                    onUpdate={(values) => handleUpdate(ENTITY_TYPES_FIELD, values)}
                    placeholder={t('appliesTo.placeholder')}
                    isMultiSelect
                    values={formValues?.entityTypes ? formValues?.entityTypes : undefined}
                    disabledValues={disabledEntityTypeValues}
                    width="full"
                    showSelectAll
                    isDisabled={isReadOnly}
                    selectAllLabel={t('appliesTo.allAssetTypes')}
                    data-testid="structured-props-select-input-applies-to"
                    optionListTestId="applies-to-options-list"
                />
                {errors.entityTypes && <FieldError>{errors.entityTypes}</FieldError>}
            </div>
            {!(isEditMode && !selectedProperty?.definition?.allowedPlatforms?.length) && (
                <Tooltip
                    title={!formValues?.allowedPlatforms?.length && t('allowedPlatforms.anyTooltip')}
                    showArrow={false}
                >
                    <div>
                        <SimpleSelect<PlatformOption>
                            label={t('allowedPlatforms.title')}
                            options={platformOptions}
                            onUpdate={(values) => handleUpdate(ALLOWED_PLATFORMS_FIELD, values)}
                            placeholder={t('allowedPlatforms.anyPlaceholder')}
                            isMultiSelect
                            showSearch
                            values={formValues?.allowedPlatforms}
                            disabledValues={disabledAllowedPlatformValues}
                            isDisabled={isReadOnly}
                            width="full"
                            renderCustomOptionText={(option) => (
                                <PlatformOptionLabel>
                                    <PlatformIcon platform={option.platform} size={16} styles={{ padding: 0 }} />
                                    <span>{option.label}</span>
                                </PlatformOptionLabel>
                            )}
                        />
                    </div>
                </Tooltip>
            )}
        </>
    );
};

export default StructuredPropsFormSection;
