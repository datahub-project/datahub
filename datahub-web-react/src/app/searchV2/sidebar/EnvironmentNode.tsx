import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    BrowseProvider,
    useEntityAggregation,
    useEnvironmentAggregation,
    useIsEnvironmentSelected,
} from '@app/searchV2/sidebar/BrowseContext';
import ExpandableNode from '@app/searchV2/sidebar/ExpandableNode';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
    padding-right: 8px;
`;

const EnvironmentNode = () => {
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
                            <PlatformNode />
                        </BrowseProvider>
                    ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </ExpandableNode.Body>
            }
        />
    );
};

export default EnvironmentNode;
