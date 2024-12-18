import { Icon, Pill, Switch, Text } from '@src/alchemy-components';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { AllowedValue, StructuredPropertyEntity } from '@src/types.generated';
import { Collapse } from 'antd';
import React, { useState } from 'react';
import { useUpdateStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import { CollapseHeader, StyledCollapse, StyledFormItem, TogglesContainer } from './styledComponents';
import { getDisplayName, canBeAssetBadge, StructuredProp } from './utils';

const SCHEMA_FIELD_URN = 'urn:li:entityType:datahub.schemaField';

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
                    <Icon icon="ChevronRight" color="gray" size="4xl" rotate={isActive ? '90' : '0'} />
                )}
                expandIconPosition="end"
                defaultActiveKey={[1]}
            >
                <Collapse.Panel
                    key={1}
                    header={
                        <CollapseHeader>
                            <Text weight="bold" color="gray">
                                Display Preferences
                            </Text>
                        </CollapseHeader>
                    }
                    forceRender
                >
                    <TogglesContainer>
                        <StyledFormItem name={['settings', 'isHidden']}>
                            <Switch
                                label="Hide Property"
                                size="sm"
                                checked={formValues?.settings?.isHidden}
                                onChange={(e) => handleDisplaySettingChange('isHidden', e.target.checked)}
                                labelHoverText="If enabled, this property will be hidden everywhere"
                                data-testid="structured-props-hide-switch"
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInSearchFilters']}>
                            <Switch
                                label="Show in Search Filters"
                                size="sm"
                                checked={formValues?.settings?.showInSearchFilters}
                                onChange={(e) => handleDisplaySettingChange('showInSearchFilters', e.target.checked)}
                                isDisabled={formValues?.settings?.isHidden}
                                labelHoverText="If enabled, this property will appear in search filters"
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInAssetSummary']}>
                            <Switch
                                label="Show in Asset Sidebar"
                                size="sm"
                                checked={formValues?.settings?.showInAssetSummary}
                                onChange={(e) => handleDisplaySettingChange('showInAssetSummary', e.target.checked)}
                                isDisabled={formValues?.settings?.isHidden}
                                labelHoverText="If enabled, this property will appear in asset sidebar"
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showAsAssetBadge']}>
                            <Switch
                                label="Show as Asset Badge"
                                size="sm"
                                checked={formValues?.settings?.showAsAssetBadge === true}
                                onChange={(e) => {
                                    if (badgeProperty && e.target.checked) setShowReplaceBadge(true);
                                    else handleDisplaySettingChange('showAsAssetBadge', e.target.checked);
                                }}
                                isDisabled={
                                    !hasAssetBadgeEnabled &&
                                    (formValues?.settings?.isHidden ||
                                        !canBeAssetBadge(selectedValueType, allowedValues))
                                }
                                labelHoverText="If enabled, this property will appear as asset badge"
                                disabledHoverText="Only Text or Number property types with allowed values defined can appear as an asset badge."
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInColumnsTable']}>
                            <Switch
                                label="Show in Columns Table"
                                size="sm"
                                checked={formValues?.settings?.showInColumnsTable}
                                onChange={(e) => handleDisplaySettingChange('showInColumnsTable', e.target.checked)}
                                isDisabled={
                                    !showInColumnsTable && (formValues?.settings?.isHidden || !hasColumnEntityType)
                                }
                                labelHoverText="If enabled, this property will appear as a column in the Columns table for Datasets"
                                disabledHoverText="Property must apply to Columns in order to show in columns table."
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
                        handleDisplaySettingChange('showAsAssetBadge', true);
                        setShowReplaceBadge(false);
                        updateBadgePropertyToOff();
                    }}
                    confirmButtonText="Update"
                    modalTitle="Update Property"
                    modalText={
                        <p>
                            <span>Another property </span>
                            <Pill
                                label={getDisplayName(badgeProperty)}
                                size="sm"
                                colorScheme="violet"
                                clickable={false}
                            />
                            &nbsp;is already being shown on asset previews, but only one property is allowed at a time.
                            Do you want to replace the current property? This will hide {getDisplayName(badgeProperty)}{' '}
                            on all asset previews.
                        </p>
                    }
                />
            )}
        </>
    );
};

export default DisplayPreferences;
