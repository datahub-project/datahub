import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { VscTriangleDown, VscTriangleRight } from 'react-icons/vsc';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { AggregationMetadata, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getFilterIconAndLabel } from '../filters/utils';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import useBrowseV2Query from './useBrowseV2Query';
import useToggle from './useToggle';
import BrowseNode from './BrowseNode';

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const PlatformIconContainer = styled.div`
    width: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
`;
const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const BrowseGroupListContainer = styled.div`
    background: white;
    border-radius: 8px;
    padding-bottom: 8px;
    padding-right: 8px;
`;

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata;
};

const PlatformNode = ({ entityAggregation, environmentAggregation, platformAggregation }: Props) => {
    const entityType = entityAggregation.value as EntityType;
    const environment = environmentAggregation?.value;
    const platform = platformAggregation.value;
    const registry = useEntityRegistry();

    const { icon, label } = getFilterIconAndLabel(
        PLATFORM_FILTER_NAME,
        platformAggregation.value,
        registry,
        platformAggregation.entity ?? null,
        16,
    );

    const { isOpen, toggle } = useToggle();

    const { loaded, error, groups, pathResult } = useBrowseV2Query({
        entityType,
        environment,
        platform,
        path: [],
        skip: !isOpen,
    });

    const color = ANTD_GRAY[9];

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        {isOpen ? <VscTriangleDown style={{ color }} /> : <VscTriangleRight style={{ color }} />}
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <Title color={color}>{label}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {!!groups?.length && (
                        <BrowseGroupListContainer>
                            {groups.map((group) => (
                                <BrowseNode
                                    key={group.name}
                                    entityAggregation={entityAggregation}
                                    environmentAggregation={environmentAggregation}
                                    platformAggregation={platformAggregation}
                                    browseResultGroup={group}
                                    path={[...pathResult, group.name]}
                                />
                            ))}
                        </BrowseGroupListContainer>
                    )}
                </ExpandableNode.Body>
            }
        />
    );
};

export default PlatformNode;
