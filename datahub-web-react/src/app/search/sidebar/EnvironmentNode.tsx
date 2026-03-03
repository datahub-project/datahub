import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    BrowseProvider,
    useEntityAggregation,
    useEnvironmentAggregation,
    useIsEnvironmentSelected,
} from '@app/search/sidebar/BrowseContext';
import ExpandableNode from '@app/search/sidebar/ExpandableNode';
import PlatformNode from '@app/search/sidebar/PlatformNode';
import SidebarLoadingError from '@app/search/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/search/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/search/sidebar/useSidebarAnalytics';
import { PLATFORM_FILTER_NAME } from '@app/search/utils/constants';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';

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
