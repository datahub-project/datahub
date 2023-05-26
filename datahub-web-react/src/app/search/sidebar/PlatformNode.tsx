import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
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
import useIntersect from '../../shared/useIntersect';

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
    const registry = useEntityRegistry();

    const { icon, label } = getFilterIconAndLabel(
        PLATFORM_FILTER_NAME,
        platformAggregation.value,
        registry,
        platformAggregation.entity ?? null,
        16,
    );

    const { isOpen, toggle } = useToggle();

    const skip = !isOpen;

    // This is the seed, we don't know how many <BrowseNodeList/> we need until this query comes back
    const { loaded, error, groups, pathResult, fetchNextPage } = useBrowseV2Query({
        entityType: entityAggregation.value as EntityType,
        environment: environmentAggregation?.value,
        platform: platformAggregation.value,
        path: [],
        skip,
    });

    const color = ANTD_GRAY[9];

    const { observableRef } = useIntersect({ skip, initialDelay: 500, onIntersect: fetchNextPage });

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.Triangle isOpen={isOpen} />
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <Title color={color}>{label}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {!!groups.length && (
                        // todo - move this to a new component that handles the pagination
                        // we don't want to deal with pagination both here and in BrowseNode right
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
                            <div ref={observableRef} style={{ width: '1px', height: '1px' }} />
                        </BrowseGroupListContainer>
                    )}
                </ExpandableNode.Body>
            }
        />
    );
};

export default PlatformNode;
