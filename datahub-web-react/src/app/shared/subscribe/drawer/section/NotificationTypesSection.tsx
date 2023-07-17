import React, { useState, Key } from 'react';
import styled from 'styled-components/macro';
import { Tree, Typography } from 'antd';
import { DataNode } from 'antd/es/tree';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { getTreeDataForEntity } from '../utils';
import { useFormDispatchContext, useFormStateContext } from '../form/context';

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
    const { checkedKeys } = useFormStateContext();
    const dispatch = useFormDispatchContext();
    const { entityType } = useEntityData();
    const [expandedKeys, setExpandedKeys] = useState<Key[]>([]);
    const [autoExpandParent, setAutoExpandParent] = useState<boolean>(true);

    const onExpand = (expandedKeysValue: Key[]) => {
        setExpandedKeys(expandedKeysValue);
        setAutoExpandParent(false);
    };

    const onCheck = (checkedKeysValue: any, _info: any) => {
        dispatch({ type: 'setCheckedKeys', payload: checkedKeysValue as Key[] });
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
                    treeData={treeData}
                />
            </TreeContainer>
        </NotificationTypesContainer>
    );
}
