import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { DownCircleOutlined, UpCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { AggregationMetadata, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import EnvironmentNode from './EnvironmentNode';
import useAggregationsQuery from './useAggregationsQuery';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import PlatformNode from './PlatformNode';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';

const Title = styled(Typography.Text)`
    font-size: 16px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const childFacets = [ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME];

type Props = {
    entityAggregation: AggregationMetadata;
};

const EntityNode = ({ entityAggregation }: Props) => {
    const entityType = entityAggregation.value as EntityType;
    const registry = useEntityRegistry();

    const { isOpen, toggle } = useToggle();

    const { loaded, error, environmentAggregations, platformAggregations } = useAggregationsQuery({
        skip: !isOpen,
        entityType: entityAggregation.value as EntityType,
        facets: childFacets,
    });

    const hasMultipleEnvironments = environmentAggregations.length > 1;
    const color = isOpen ? ANTD_GRAY[9] : ANTD_GRAY[7];

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={toggle} style={{ paddingTop: '16px' }}>
                    <ExpandableNode.HeaderLeft>
                        {registry.getIcon(entityType, 16, IconStyleType.HIGHLIGHT, color)}
                        <Title color={color}>{registry.getCollectionName(entityType)}</Title>
                        <Count color={color}>{formatNumber(entityAggregation.count)}</Count>
                    </ExpandableNode.HeaderLeft>
                    {isOpen ? <UpCircleOutlined style={{ color }} /> : <DownCircleOutlined style={{ color }} />}
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {hasMultipleEnvironments
                        ? environmentAggregations.map((environmentAggregation) => (
                              <EnvironmentNode
                                  key={environmentAggregation.value}
                                  entityAggregation={entityAggregation}
                                  environmentAggregation={environmentAggregation}
                              />
                          ))
                        : platformAggregations.map((platform) => (
                              <PlatformNode
                                  key={platform.value}
                                  entityAggregation={entityAggregation}
                                  environmentAggregation={null}
                                  platformAggregation={platform}
                              />
                          ))}
                    {error && <SidebarLoadingError />}
                </ExpandableNode.Body>
            }
        />
    );
};

export default EntityNode;
