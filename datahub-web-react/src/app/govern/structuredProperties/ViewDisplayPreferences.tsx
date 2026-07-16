import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Collapse } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import {
    CheckboxContainer,
    CollapseHeader,
    CompoundedItemWrapper,
    StyledCollapse,
    StyledFormItem,
    StyledFormSubItem,
    TogglesContainer,
} from '@app/govern/structuredProperties/styledComponents';
import { Checkbox, Icon, Switch, Text } from '@src/alchemy-components';
import { StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    propEntity: StructuredPropertyEntity;
}

const ViewDisplayPreferences = ({ propEntity }: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const theme = useTheme();
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
                                checked={propEntity?.settings?.isHidden}
                                labelStyle={{ fontSize: 12, color: theme.colors.textSecondary, fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInSearchFilters']}>
                            <Switch
                                label={t('display.showInSearchFilters')}
                                size="sm"
                                checked={propEntity?.settings?.showInSearchFilters}
                                labelStyle={{ fontSize: 12, color: theme.colors.textSecondary, fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <CompoundedItemWrapper>
                            <StyledFormItem name={['settings', 'showInAssetSummary']}>
                                <Switch
                                    label={t('display.showInAssetSidebar')}
                                    size="sm"
                                    checked={propEntity?.settings?.showInAssetSummary}
                                    labelStyle={{ fontSize: 12, color: theme.colors.textSecondary, fontWeight: 700 }}
                                    isDisabled
                                />
                            </StyledFormItem>

                            {propEntity?.settings?.showInAssetSummary && (
                                <StyledFormSubItem name={['settings', 'hideInAssetSummaryWhenEmpty']}>
                                    <CheckboxContainer>
                                        <Checkbox
                                            label={t('display.hideWhenEmpty')}
                                            isChecked={propEntity?.settings?.hideInAssetSummaryWhenEmpty}
                                            labelTooltip={t('display.hideWhenEmptyTooltip')}
                                            size="sm"
                                            gap="2px"
                                            justifyContent="flex-start"
                                            shouldHandleLabelClicks
                                            isDisabled
                                        />
                                    </CheckboxContainer>
                                </StyledFormSubItem>
                            )}
                        </CompoundedItemWrapper>
                        <StyledFormItem name={['settings', 'showAsAssetBadge']}>
                            <Switch
                                label={t('display.showAsAssetBadge')}
                                size="sm"
                                checked={propEntity?.settings?.showAsAssetBadge === true}
                                labelStyle={{ fontSize: 12, color: theme.colors.textSecondary, fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInColumnsTable']}>
                            <Switch
                                label={t('display.showInColumnsTable')}
                                size="sm"
                                checked={propEntity?.settings?.showInColumnsTable}
                                labelStyle={{ fontSize: 12, color: theme.colors.textSecondary, fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                    </TogglesContainer>
                </Collapse.Panel>
            </StyledCollapse>
        </>
    );
};

export default ViewDisplayPreferences;
