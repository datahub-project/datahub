import React, { useState, Key } from 'react';
import styled from 'styled-components/macro';
import { Tree, Typography } from 'antd';
import { DataNode } from 'antd/es/tree';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { getDefaultSelectedKeys, getTreeDataForEntity } from '../utils';

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
    const { entityType } = useEntityData();
    const defaultCheckedKeys: string[] = getDefaultSelectedKeys(entityType);
    const [expandedKeys, setExpandedKeys] = useState<Key[]>([]);
    const [checkedKeys, setCheckedKeys] = useState<Key[]>(defaultCheckedKeys);
    const [selectedKeys, setSelectedKeys] = useState<Key[]>(defaultCheckedKeys);
    const [autoExpandParent, setAutoExpandParent] = useState<boolean>(true);

    const onExpand = (expandedKeysValue: Key[]) => {
        setExpandedKeys(expandedKeysValue);
        setAutoExpandParent(false);
    };

    const onCheck = (checkedKeysValue: any, _info: any) => {
        setCheckedKeys(checkedKeysValue as Key[]);
    };

    const onSelect = (selectedKeysValue: Key[], _info: any) => {
        setSelectedKeys(selectedKeysValue);
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
                    autoExpandParent={autoExpandParent}
                    onCheck={onCheck}
                    checkedKeys={checkedKeys}
                    onSelect={onSelect}
                    selectedKeys={selectedKeys}
                    treeData={treeData}
                />
            </TreeContainer>
        </NotificationTypesContainer>
    );
}
