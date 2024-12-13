import { colors, Icon, Switch, Text } from '@src/alchemy-components';
import { StructuredPropertyEntity } from '@src/types.generated';
import { Collapse } from 'antd';
import React from 'react';
import { CollapseHeader, StyledCollapse, StyledFormItem, TogglesContainer } from './styledComponents';

interface Props {
    propEntity: StructuredPropertyEntity;
}

const ViewDisplayPreferences = ({ propEntity }: Props) => {
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
                                checked={propEntity?.settings?.isHidden}
                                labelStyle={{ fontSize: 12, color: colors.gray[1700], fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInSearchFilters']}>
                            <Switch
                                label="Show in Search Filters"
                                size="sm"
                                checked={propEntity?.settings?.showInSearchFilters}
                                labelStyle={{ fontSize: 12, color: colors.gray[1700], fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInAssetSummary']}>
                            <Switch
                                label="Show in Asset Sidebar"
                                size="sm"
                                checked={propEntity?.settings?.showInAssetSummary}
                                labelStyle={{ fontSize: 12, color: colors.gray[1700], fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showAsAssetBadge']}>
                            <Switch
                                label="Show as Asset Badge"
                                size="sm"
                                checked={propEntity?.settings?.showAsAssetBadge === true}
                                labelStyle={{ fontSize: 12, color: colors.gray[1700], fontWeight: 700 }}
                                isDisabled
                            />
                        </StyledFormItem>
                        <StyledFormItem name={['settings', 'showInColumnsTable']}>
                            <Switch
                                label="Show in Columns Table"
                                size="sm"
                                checked={propEntity?.settings?.showInColumnsTable}
                                labelStyle={{ fontSize: 12, color: colors.gray[1700], fontWeight: 700 }}
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
