import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';

import CollapsibleSection from '@app/govern/structuredProperties/CollapsibleSection';
import {
    CheckboxContainer,
    CompoundedItemWrapper,
    SettingSubItem,
    TogglesContainer,
} from '@app/govern/structuredProperties/styledComponents';
import { StructuredProp, canBeAssetBadge, getDisplayName } from '@app/govern/structuredProperties/utils';
import { Checkbox, Pill, Switch } from '@src/alchemy-components';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { AllowedValueInput, StructuredPropertyEntity } from '@src/types.generated';

const SCHEMA_FIELD_URN = 'urn:li:entityType:datahub.schemaField';
const DISPLAY_SETTING = {
    isHidden: 'isHidden',
    showInSearchFilters: 'showInSearchFilters',
    showInAssetSummary: 'showInAssetSummary',
    hideInAssetSummaryWhenEmpty: 'hideInAssetSummaryWhenEmpty',
    showAsAssetBadge: 'showAsAssetBadge',
    showInColumnsTable: 'showInColumnsTable',
} as const;

type Props = {
    formValues: StructuredProp | undefined;
    isReadOnly: boolean;
    handleDisplaySettingChange: (settingField: string, value: boolean) => void;
    selectedValueType: string;
    allowedValues?: AllowedValueInput[];
    badgeProperty?: StructuredPropertyEntity;
};

const DisplayPreferences = ({
    formValues,
    isReadOnly,
    handleDisplaySettingChange,
    selectedValueType,
    allowedValues,
    badgeProperty,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tc } = useTranslation('common.actions');
    const [showReplaceBadge, setShowReplaceBadge] = useState(false);

    const handleReplaceClose = () => {
        setShowReplaceBadge(false);
    };

    const hasAssetBadgeEnabled = formValues?.settings?.showAsAssetBadge;
    const showInColumnsTable = formValues?.settings?.showInColumnsTable;
    const hasColumnEntityType = formValues?.entityTypes?.includes(SCHEMA_FIELD_URN);

    return (
        <>
            <CollapsibleSection
                title={t('display.title')}
                defaultOpen
                dataTestId="structured-props-display-preferences"
            >
                <TogglesContainer>
                    <Switch
                        label={t('display.hideProperty')}
                        checked={formValues?.settings?.isHidden}
                        onChange={(e) => handleDisplaySettingChange(DISPLAY_SETTING.isHidden, e.target.checked)}
                        isDisabled={isReadOnly}
                        labelHoverText={t('display.hidePropertyTooltip')}
                        data-testid="structured-props-hide-switch"
                    />
                    <Switch
                        label={t('display.showInSearchFilters')}
                        checked={formValues?.settings?.showInSearchFilters ?? false}
                        onChange={(e) =>
                            handleDisplaySettingChange(DISPLAY_SETTING.showInSearchFilters, e.target.checked)
                        }
                        isDisabled={isReadOnly || formValues?.settings?.isHidden}
                        labelHoverText={t('display.showInSearchFiltersTooltip')}
                        data-testid="structured-props-show-in-search-filters-switch"
                    />
                    <CompoundedItemWrapper>
                        <Switch
                            label={t('display.showInAssetSidebar')}
                            checked={formValues?.settings?.showInAssetSummary}
                            onChange={(e) =>
                                handleDisplaySettingChange(DISPLAY_SETTING.showInAssetSummary, e.target.checked)
                            }
                            isDisabled={isReadOnly || formValues?.settings?.isHidden}
                            labelHoverText={t('display.showInAssetSidebarTooltip')}
                            data-testid="structured-props-show-in-asset-summary-switch"
                        />
                        {formValues?.settings?.showInAssetSummary && (
                            <SettingSubItem>
                                <CheckboxContainer>
                                    <Checkbox
                                        label={t('display.hideWhenEmpty')}
                                        isChecked={formValues?.settings?.hideInAssetSummaryWhenEmpty}
                                        isDisabled={isReadOnly}
                                        labelTooltip={t('display.hideWhenEmptyTooltip')}
                                        size="sm"
                                        gap="2px"
                                        onCheckboxChange={(isChecked) =>
                                            handleDisplaySettingChange(
                                                DISPLAY_SETTING.hideInAssetSummaryWhenEmpty,
                                                isChecked,
                                            )
                                        }
                                        justifyContent="flex-start"
                                        dataTestId="structured-props-hide-in-asset-summary-when-empty-checkbox"
                                        shouldHandleLabelClicks
                                    />
                                </CheckboxContainer>
                            </SettingSubItem>
                        )}
                    </CompoundedItemWrapper>
                    <Switch
                        label={t('display.showAsAssetBadge')}
                        checked={formValues?.settings?.showAsAssetBadge === true}
                        onChange={(e) => {
                            if (badgeProperty && e.target.checked) setShowReplaceBadge(true);
                            else handleDisplaySettingChange(DISPLAY_SETTING.showAsAssetBadge, e.target.checked);
                        }}
                        isDisabled={
                            isReadOnly ||
                            (!hasAssetBadgeEnabled &&
                                (formValues?.settings?.isHidden || !canBeAssetBadge(selectedValueType, allowedValues)))
                        }
                        labelHoverText={t('display.showAsAssetBadgeTooltip')}
                        disabledHoverText={t('display.showAsAssetBadgeDisabledTooltip')}
                    />
                    <Switch
                        label={t('display.showInColumnsTable')}
                        checked={formValues?.settings?.showInColumnsTable}
                        onChange={(e) =>
                            handleDisplaySettingChange(DISPLAY_SETTING.showInColumnsTable, e.target.checked)
                        }
                        isDisabled={
                            isReadOnly ||
                            (!showInColumnsTable && (formValues?.settings?.isHidden || !hasColumnEntityType))
                        }
                        labelHoverText={t('display.showInColumnsTableTooltip')}
                        disabledHoverText={t('display.showInColumnsTableDisabledTooltip')}
                        data-testid="structured-props-show-in-columns-table-switch"
                    />
                </TogglesContainer>
            </CollapsibleSection>
            {badgeProperty && (
                <ConfirmationModal
                    isOpen={showReplaceBadge}
                    handleClose={handleReplaceClose}
                    handleConfirm={() => {
                        handleDisplaySettingChange(DISPLAY_SETTING.showAsAssetBadge, true);
                        setShowReplaceBadge(false);
                    }}
                    confirmButtonText={tc('update')}
                    modalTitle={t('display.updatePropertyTitle')}
                    modalText={
                        <p>
                            <Trans
                                t={t}
                                i18nKey="display.replaceBadgeConfirmation"
                                components={{
                                    pill: (
                                        <Pill
                                            label={getDisplayName(badgeProperty)}
                                            size="sm"
                                            color="primary"
                                            clickable={false}
                                        />
                                    ),
                                }}
                                values={{ name: getDisplayName(badgeProperty) }}
                            />
                        </p>
                    }
                />
            )}
        </>
    );
};

export default DisplayPreferences;
