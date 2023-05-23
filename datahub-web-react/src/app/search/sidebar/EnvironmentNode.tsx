import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { VscTriangleDown, VscTriangleRight } from 'react-icons/vsc';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { AggregationMetadata, EntityType } from '../../../types.generated';
import useAggregationsQuery from './useAggregationsQuery';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import PlatformNode from './PlatformNode';

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    user-select: none;
    padding-top: 8px;
`;

const HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const Body = styled.div``;

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata;
};

const childrenFacets = [PLATFORM_FILTER_NAME];

const EnvironmentNode = ({ entityAggregation, environmentAggregation }: Props) => {
    const entityType = entityAggregation.value as EntityType;
    const environment = environmentAggregation.value;
    const depth = 0;
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const color = ANTD_GRAY[9];

    const [getAggregations, { loaded, error, platformAggregations }] = useAggregationsQuery({
        entityType,
        environment,
        facets: childrenFacets,
        skip: !isOpen,
    });

    const onClickHeader = () => {
        getAggregations();
        setIsOpen((current) => !current);
    };

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            depth={depth}
            header={
                <Header onClick={onClickHeader}>
                    <HeaderLeft>
                        {isOpen ? <VscTriangleDown style={{ color }} /> : <VscTriangleRight style={{ color }} />}
                        <Title color={color}>{environmentAggregation.value}</Title>
                    </HeaderLeft>
                    <Count color={color}>{formatNumber(environmentAggregation.count)}</Count>
                </Header>
            }
            body={
                <Body>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {platformAggregations.map((platform) => (
                        <PlatformNode
                            key={platform.value}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platform}
                            depth={depth + 1}
                        />
                    ))}
                </Body>
            }
        />
    );
};

export default EnvironmentNode;
