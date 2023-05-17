import React, { memo, useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { DownCircleOutlined, UpCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import EnvironmentNode from './EnvironmentNode';
import useEnvironmentsQuery from './useEnvironmentsQuery';
import DelayedLoading from './DelayedLoading';

const Header = styled.div<{ isOpen: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    user-select: none;
    padding-top: 16px;
    border-bottom: ${(props) => `1px solid ${props.isOpen ? ANTD_GRAY[2] : ANTD_GRAY[4]}`};
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

const Body = styled.div``;

type Props = {
    entityType: EntityType;
    count: number;
};

const EntityNode = ({ entityType, count }: Props) => {
    const registry = useEntityRegistry();
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const onClickHeader = useCallback(() => setIsOpen((current) => !current), []);
    const color = isOpen ? ANTD_GRAY[9] : ANTD_GRAY[7];
    const [fetchAggregations, { loading, error, environments }] = useEnvironmentsQuery();

    useEffect(() => {
        if (!isOpen) return;
        fetchAggregations(entityType);
    }, [entityType, fetchAggregations, isOpen]);

    // For local testing as we're building out the browsev2 sidebar
    const showEnvironmentOverride = true;
    const showEnvironment = environments.length > 1 || showEnvironmentOverride;

    return (
        <ExpandableNode
            isOpen={isOpen}
            header={
                <Header isOpen={isOpen} onClick={onClickHeader}>
                    <HeaderLeft>
                        {registry.getIcon(entityType, 16, IconStyleType.HIGHLIGHT, color)}
                        <Title color={color}>{registry.getCollectionName(entityType as EntityType)}</Title>
                        <Count color={color}>{formatNumber(count)}</Count>
                    </HeaderLeft>
                    {isOpen ? <UpCircleOutlined style={{ color }} /> : <DownCircleOutlined style={{ color }} />}
                </Header>
            }
            body={
                <Body>
                    {loading && <DelayedLoading />}
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {showEnvironment &&
                        environments.map((env) => (
                            <EnvironmentNode key={env.value} environment={env.value} count={env.count} />
                        ))}
                </Body>
            }
        />
    );
};

export default memo(EntityNode);
