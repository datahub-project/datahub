import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
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
    useEnvironmentAggregation,
    useIsEnvironmentSelected,
} from './BrowseContext';
import useSidebarAnalytics from './useSidebarAnalytics';

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
    padding-right: 8px;
`;

interface EntityNodeProps {
    sortBy: string;
}

const EnvironmentNode: React.FC<EntityNodeProps> = ({ sortBy }) => {
    const isSelected = useIsEnvironmentSelected();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useEnvironmentAggregation();
    const { count } = environmentAggregation;
    const { trackToggleNodeEvent } = useSidebarAnalytics();
    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'environment'),
    });

    const onClickHeader = () => {
        if (count) toggle();
    };

    const { loaded, error, platformAggregations, retry } = useAggregationsQuery({
        skip: !isOpen,
        facets: [PLATFORM_FILTER_NAME],
    });

    const color = '#000';

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={onClickHeader}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={!!count}
                            onClick={onClickHeader}
                        />
                        <ExpandableNode.Title color={color} size={14}>
                            {environmentAggregation?.value}
                        </ExpandableNode.Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {platformAggregations?.map((platformAggregation) => (
                        <BrowseProvider
                            key={platformAggregation.value}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                        >
                            <PlatformNode sortBy={sortBy} />
                        </BrowseProvider>
                    ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </ExpandableNode.Body>
            }
        />
    );
};

export default EnvironmentNode;
