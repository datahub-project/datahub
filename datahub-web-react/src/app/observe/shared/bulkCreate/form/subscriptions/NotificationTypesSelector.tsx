import { colors } from '@components';
import { CaretDown, CaretRight } from '@phosphor-icons/react';
import { Collapse, Tree } from 'antd';
import { DataNode } from 'antd/lib/tree';
import { Check } from 'phosphor-react';
import React, { Key } from 'react';
import styled from 'styled-components';

import { generateSummaryText } from '@app/observe/shared/bulkCreate/form/subscriptions/NotificationTypesSelector.utils';
import { NotificationSettingsSection } from '@app/observe/shared/bulkCreate/form/subscriptions/notificationsConfiguration/NotificationSettingsSection';

const TreeContainer = styled.div`
    margin-left: 16px;
    margin-top: 8px;

    .ant-tree-checkbox .ant-tree-checkbox-inner {
        border-color: ${colors.gray[300]};
    }

    .ant-tree-node-content-wrapper {
        background: none;
        cursor: auto;

        &:hover {
            background: none;
        }
    }

    .ant-tree .ant-tree-node-content-wrapper.ant-tree-node-selected {
        background-color: transparent;
    }
`;

const StyledCollapse = styled(Collapse)`
    .ant-collapse-header {
        padding: 8px 0 !important;
        padding-bottom: 0px !important;
        align-items: center;
    }

    .ant-collapse-content-box {
        padding-top: 0px !important;
        padding-bottom: 0px !important;
    }

    .ant-collapse-arrow {
        margin-right: 8px !important;
        line-height: 32px;
    }

    &.ant-collapse {
        border-radius: 0 !important;
        border: none;
        background: none;
    }

    .ant-collapse-item {
        border-radius: 0 !important;
        border: none;

        &:last-child {
            border-bottom: none;
        }
    }
`;

const CollapseHeader = styled.div`
    display: flex;
    align-items: center;
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[500]};
`;

const StyledIcon = styled.div`
    svg {
        height: 16px;
        width: 16px;
        color: ${colors.gray[500]};
    }
`;

const SummaryLabel = styled.div`
    margin-left: 0;
    font-size: 14px;
    color: ${colors.gray[600]};
    font-weight: 600;
`;

const SummaryLabelCheck = styled(Check)`
    position: relative;
    top: 2px;
    margin-right: 4px;
`;

interface NotificationTypesSelectorProps {
    checkedKeys: Key[];
    onCheck: (checkedKeysValue: any) => void;
    notificationTypesExpanded: boolean;
    setNotificationTypesExpanded: (expanded: boolean) => void;
    treeData: DataNode[];
    isPersonal: boolean;
    groupUrn?: string;
    panelKey: string;
}

export const NotificationTypesSelector: React.FC<NotificationTypesSelectorProps> = ({
    checkedKeys,
    onCheck,
    notificationTypesExpanded,
    setNotificationTypesExpanded,
    treeData,
    isPersonal,
    groupUrn,
    panelKey,
}) => {
    return (
        <div style={{ marginTop: 16 }}>
            {!notificationTypesExpanded && checkedKeys.length > 0 && (
                <SummaryLabel>
                    {checkedKeys.length ? <SummaryLabelCheck size={16} /> : null} {generateSummaryText(checkedKeys)}
                </SummaryLabel>
            )}
            <StyledCollapse
                ghost
                expandIcon={({ isActive }) => <StyledIcon>{isActive ? <CaretDown /> : <CaretRight />}</StyledIcon>}
                onChange={(activeKey) => {
                    setNotificationTypesExpanded(Array.isArray(activeKey) ? activeKey.length > 0 : !!activeKey);
                }}
            >
                <Collapse.Panel header={<CollapseHeader>Select Notification Types</CollapseHeader>} key={panelKey}>
                    <TreeContainer>
                        <Tree
                            checkable
                            checkedKeys={checkedKeys}
                            onCheck={onCheck}
                            treeData={treeData}
                            defaultExpandAll
                        />
                    </TreeContainer>
                </Collapse.Panel>
            </StyledCollapse>

            {/* Notification Settings Section */}
            <NotificationSettingsSection isPersonal={isPersonal} groupUrn={groupUrn} />
        </div>
    );
};
