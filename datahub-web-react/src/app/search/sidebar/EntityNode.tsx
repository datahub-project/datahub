import React, { memo, useCallback } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { DownCircleOutlined, UpCircleOutlined } from '@ant-design/icons';
import { VscTriangleRight } from 'react-icons/vsc';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';

const Header = styled.div<{ isSelected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    user-select: none;
    padding-top: 18px;
    border-bottom: ${(props) => `1px solid ${props.isSelected ? ANTD_GRAY[2] : ANTD_GRAY[4]}`};
`;

const HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Title = styled(Typography.Text)`
    font-size: 16px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const Body = styled.div`
    display: flex;
    flex-direction: column;
    padding: 8px;
`;

type Props = {
    entityType: EntityType;
    count: number;
    isSelected: boolean;
    onSelect: (entityType: EntityType) => void;
};

const EntityNode = ({ entityType, count, isSelected, onSelect }: Props) => {
    const registry = useEntityRegistry();
    const onClick = useCallback(() => onSelect(entityType), [entityType, onSelect]);
    const color = isSelected ? ANTD_GRAY[9] : ANTD_GRAY[7];

    return (
        <ExpandableNode
            isOpen={isSelected}
            header={
                <Header isSelected={isSelected} onClick={onClick}>
                    <HeaderLeft>
                        {registry.getIcon(entityType, 16, IconStyleType.HIGHLIGHT, color)}
                        <Title color={color}>{registry.getCollectionName(entityType as EntityType)}</Title>
                        <Count color={color}>{formatNumber(count)}</Count>
                    </HeaderLeft>
                    {isSelected ? <UpCircleOutlined style={{ color }} /> : <DownCircleOutlined style={{ color }} />}
                </Header>
            }
            body={
                <Body>
                    {/* TODO - just sample body contents for now */}
                    <div>
                        {['dbt', 'Looker', 'Redshift'].map((platformName) => (
                            <div key={platformName}>
                                <VscTriangleRight /> {platformName}
                            </div>
                        ))}
                    </div>
                </Body>
            }
        />
    );
};

export default memo(EntityNode);
