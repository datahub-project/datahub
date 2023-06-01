import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import useAggregationsQuery from './useAggregationsQuery';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import PlatformNode from './PlatformNode';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsEnvironmentSelected,
    useMaybeEnvironmentAggregation,
} from './BrowseContext';

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const EnvironmentNode = () => {
    const isSelected = useIsEnvironmentSelected();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const { isOpen, toggle } = useToggle(isSelected);

    const { loaded, error, platformAggregations } = useAggregationsQuery({
        skip: !isOpen,
        facets: [PLATFORM_FILTER_NAME],
    });

    const color = ANTD_GRAY[9];

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen}
                            isVisible={!!environmentAggregation?.count}
                            onClick={toggle}
                        />
                        <Title color={color}>{environmentAggregation?.value}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(environmentAggregation?.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {platformAggregations.map((platformAggregation) => (
                        <BrowseProvider
                            key={platformAggregation.value}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                        >
                            <PlatformNode />
                        </BrowseProvider>
                    ))}
                    {error && <SidebarLoadingError />}
                </ExpandableNode.Body>
            }
        />
    );
};

export default EnvironmentNode;
