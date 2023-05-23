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

const Body = styled.div``;

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata;
    depth: number;
};

const path = ['/'];

const PlatformNode = ({ entityAggregation, environmentAggregation, platformAggregation, depth }: Props) => {
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

    const [getBrowse, { groups, loaded, error }] = useBrowseV2Query({ entityType, environment, platform, path });

    const { isOpen, toggle } = useToggle({ onRequestOpen: getBrowse });

    const color = ANTD_GRAY[9];

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            depth={depth}
            header={
                <Header onClick={toggle}>
                    <HeaderLeft>
                        {isOpen ? <VscTriangleDown style={{ color }} /> : <VscTriangleRight style={{ color }} />}
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <Title color={color}>{label}</Title>
                    </HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </Header>
            }
            body={
                <Body>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {groups?.map((group) => (
                        <div>{JSON.stringify(group)}</div>
                    ))}
                </Body>
            }
        />
    );
};

export default PlatformNode;
