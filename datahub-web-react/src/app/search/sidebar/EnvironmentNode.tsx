import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { AggregationMetadata, EntityType } from '../../../types.generated';
import useAggregationsQuery from './useAggregationsQuery';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import PlatformNode from './PlatformNode';
import useToggle from './useToggle';
import SidebarLoadingError from './SidebarLoadingError';

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const childFacets = [PLATFORM_FILTER_NAME];

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata;
};

const EnvironmentNode = ({ entityAggregation, environmentAggregation }: Props) => {
    const { isOpen, toggle } = useToggle();

    const { loaded, error, platformAggregations } = useAggregationsQuery({
        skip: !isOpen,
        entityType: entityAggregation.value as EntityType,
        environment: environmentAggregation.value,
        facets: childFacets,
    });

    const color = ANTD_GRAY[9];

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.Triangle isOpen={isOpen} />
                        <Title color={color}>{environmentAggregation.value}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(environmentAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {platformAggregations.map((platform) => (
                        <PlatformNode
                            key={platform.value}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platform}
                        />
                    ))}
                    {error && <SidebarLoadingError />}
                </ExpandableNode.Body>
            }
        />
    );
};

export default EnvironmentNode;
