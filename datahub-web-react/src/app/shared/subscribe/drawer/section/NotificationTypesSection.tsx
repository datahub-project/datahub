import React, { Key } from 'react';
import styled from 'styled-components/macro';
import { Tree, Typography } from 'antd';
import { DataNode } from 'antd/es/tree';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { getTreeDataForEntity } from '../utils';
import { useDrawerState } from '../state/context';
import useDrawerActions from '../state/actions';

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
`;

export default function NotificationTypesSection() {
    const {
        notificationTypes: { checkedKeys, expandedKeys },
    } = useDrawerState();
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
