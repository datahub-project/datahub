import React, { Key } from 'react';
import styled from 'styled-components/macro';
import { Tree, Typography } from 'antd';
import { DataNode } from 'antd/es/tree';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { getTreeDataForEntity } from '../utils';
import useDrawerActions from '../state/actions';
import { selectCheckedKeys, selectExpandedKeys, selectHasEnabledSink, useDrawerSelector } from '../state/selectors';

const NotificationTypesContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
`;

const NotifyActorTitleContainer = styled.div`
    margin-bottom: 8px;
`;

const NotifyActorTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const TreeContainer = styled.div`
    .ant-tree-checkbox .ant-tree-checkbox-inner {
        border-color: ${ANTD_GRAY[7]};
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

export default function NotificationTypesSection() {
    const checkedKeys = useDrawerSelector(selectCheckedKeys);
    const expandedKeys = useDrawerSelector(selectExpandedKeys);
    const hasEnabledSink = useDrawerSelector(selectHasEnabledSink);
    const actions = useDrawerActions();
    const { entityType } = useEntityData();

    const onExpand = (expandedKeysValue: Key[]) => {
        actions.setExpandedNotificationTypes(expandedKeysValue);
    };

    const onCheck = (checkedKeysValue: any, _info: any) => {
        actions.setNotificationTypes(checkedKeysValue);
    };

    const treeData: DataNode[] = getTreeDataForEntity(entityType);

    return (
        <NotificationTypesContainer>
            <NotifyActorTitleContainer>
                <NotifyActorTitle>Send notifications for</NotifyActorTitle>
            </NotifyActorTitleContainer>
            <TreeContainer>
                <Tree
                    disabled={!hasEnabledSink}
                    checkable
                    onExpand={onExpand}
                    expandedKeys={expandedKeys}
                    onCheck={onCheck}
                    checkedKeys={checkedKeys}
                    treeData={treeData}
                />
            </TreeContainer>
        </NotificationTypesContainer>
    );
}
