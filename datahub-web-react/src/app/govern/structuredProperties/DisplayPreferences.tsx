import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Collapse } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';

import {
    CheckboxContainer,
    CollapseHeader,
    CompoundedItemWrapper,
    StyledCollapse,
    StyledFormItem,
    StyledFormSubItem,
    TogglesContainer,
} from '@app/govern/structuredProperties/styledComponents';
import { StructuredProp, canBeAssetBadge, getDisplayName } from '@app/govern/structuredProperties/utils';
import { Checkbox, Icon, Pill, Switch, Text } from '@src/alchemy-components';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { useUpdateStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import { AllowedValue, StructuredPropertyEntity } from '@src/types.generated';

const SCHEMA_FIELD_URN = 'urn:li:entityType:datahub.schemaField';
const DISPLAY_SETTING = {
    isHidden: 'isHidden',
    showInSearchFilters: 'showInSearchFilters',
    showInAssetSummary: 'showInAssetSummary',
    hideInAssetSummaryWhenEmpty: 'hideInAssetSummaryWhenEmpty',
    showAsAssetBadge: 'showAsAssetBadge',
    showInColumnsTable: 'showInColumnsTable',
} as const;

interface Props {
    formValues: StructuredProp | undefined;
    handleDisplaySettingChange: (settingField: string, value: boolean) => void;
    selectedValueType: string;
    refetchProperties: () => void;
    allowedValues?: AllowedValue[];
    badgeProperty?: StructuredPropertyEntity;
}

const DisplayPreferences = ({
    formValues,
    handleDisplaySettingChange,
    selectedValueType,
    refetchProperties,
    allowedValues,
    badgeProperty,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tc } = useTranslation('common.actions');
    const [updateProperty] = useUpdateStructuredPropertyMutation();
    const [showReplaceBadge, setShowReplaceBadge] = useState<boolean>(false);

    const handleReplaceClose = () => {
        setShowReplaceBadge(false);
    };

    function updateBadgePropertyToOff() {
        if (badgeProperty) {
            updateProperty({
                variables: { input: { urn: badgeProperty.urn, settings: { showAsAssetBadge: false } } },
            }).then(() => refetchProperties());
        }
    }

    const hasAssetBadgeEnabled = formValues?.settings?.showAsAssetBadge;
    const showInColumnsTable = formValues?.settings?.showInColumnsTable;
    const hasColumnEntityType = formValues?.entityTypes?.includes(SCHEMA_FIELD_URN);

    return (
        <>
            <StyledCollapse
                ghost
                expandIcon={({ isActive }) => (
                    <Icon icon={CaretRight} color="gray" size="4xl" rotate={isActive ? '90' : '0'} />
                )}
                expandIconPosition="end"
                defaultActiveKey={[1]}
            >
                <Collapse.Panel
                    key={1}
                    header={
                        <CollapseHeader>
                            <Text weight="bold" color="gray">
                                {t('display.title')}
                            </Text>
                        </CollapseHeader>
                    }
                    forceRender
                >
                    <TogglesContainer>
                        <StyledFormItem name={['settings', 'isHidden']}>
                            <Switch
                                label={t('display.hideProperty')}
                                size="sm"
                                checked={formValues?.settings?.isHidden}
                                onChange={(e) => handleDisplaySettingChange(DISPLAY_SETTING.isHidden, e.target.checked)}
                                labelHoverText={t('display.hidePropertyTooltip')}
                                data-testid="structured-props-hide-switch"
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInSearchFilters']}>
                            <Switch
                                label={t('display.showInSearchFilters')}
                                size="sm"
                                checked={formValues?.settings?.showInSearchFilters}
                                onChange={(e) =>
                                    handleDisplaySettingChange(DISPLAY_SETTING.showInSearchFilters, e.target.checked)
                                }
                                isDisabled={formValues?.settings?.isHidden}
                                labelHoverText={t('display.showInSearchFiltersTooltip')}
                            />
                        </StyledFormItem>
                        <CompoundedItemWrapper>
                            <StyledFormItem name={['settings', 'showInAssetSummary']}>
                                <Switch
                                    label={t('display.showInAssetSidebar')}
                                    size="sm"
                                    checked={formValues?.settings?.showInAssetSummary}
                                    onChange={(e) =>
                                        handleDisplaySettingChange(DISPLAY_SETTING.showInAssetSummary, e.target.checked)
                                    }
                                    isDisabled={formValues?.settings?.isHidden}
                                    labelHoverText={t('display.showInAssetSidebarTooltip')}
                                    data-testid="structured-props-show-in-asset-summary-switch"
                                />
                            </StyledFormItem>
                            {formValues?.settings?.showInAssetSummary && (
                                <StyledFormSubItem name={['settings', 'hideInAssetSummaryWhenEmpty']}>
                                    <CheckboxContainer>
                                        <Checkbox
                                            label={t('display.hideWhenEmpty')}
                                            isChecked={formValues?.settings?.hideInAssetSummaryWhenEmpty}
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
                                </StyledFormSubItem>
                            )}
                        </CompoundedItemWrapper>
                        <StyledFormItem name={['settings', 'showAsAssetBadge']}>
                            <Switch
                                label={t('display.showAsAssetBadge')}
                                size="sm"
                                checked={formValues?.settings?.showAsAssetBadge === true}
                                onChange={(e) => {
                                    if (badgeProperty && e.target.checked) setShowReplaceBadge(true);
                                    else handleDisplaySettingChange(DISPLAY_SETTING.showAsAssetBadge, e.target.checked);
                                }}
                                isDisabled={
                                    !hasAssetBadgeEnabled &&
                                    (formValues?.settings?.isHidden ||
                                        !canBeAssetBadge(selectedValueType, allowedValues))
                                }
                                labelHoverText={t('display.showAsAssetBadgeTooltip')}
                                disabledHoverText={t('display.showAsAssetBadgeDisabledTooltip')}
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInColumnsTable']}>
                            <Switch
                                label={t('display.showInColumnsTable')}
                                size="sm"
                                checked={formValues?.settings?.showInColumnsTable}
                                onChange={(e) =>
                                    handleDisplaySettingChange(DISPLAY_SETTING.showInColumnsTable, e.target.checked)
                                }
                                isDisabled={
                                    !showInColumnsTable && (formValues?.settings?.isHidden || !hasColumnEntityType)
                                }
                                labelHoverText={t('display.showInColumnsTableTooltip')}
                                disabledHoverText={t('display.showInColumnsTableDisabledTooltip')}
                                data-testid="structured-props-show-in-columns-table-switch"
                            />
                        </StyledFormItem>
                    </TogglesContainer>
                </Collapse.Panel>
            </StyledCollapse>
            {badgeProperty && (
                <ConfirmationModal
                    isOpen={showReplaceBadge}
                    handleClose={handleReplaceClose}
                    handleConfirm={() => {
                        handleDisplaySettingChange(DISPLAY_SETTING.showAsAssetBadge, true);
                        setShowReplaceBadge(false);
                        updateBadgePropertyToOff();
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
                                            color="violet"
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
