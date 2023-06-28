import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getFilterIconAndLabel } from '../filters/utils';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import useBrowsePagination from './useBrowsePagination';
import BrowseNode from './BrowseNode';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsPlatformSelected,
    useMaybeEnvironmentAggregation,
    usePlatformAggregation,
} from './BrowseContext';
import useSidebarAnalytics from './useSidebarAnalytics';

const PlatformIconContainer = styled.div`
    width: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
`;
const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
    padding-right: 8px;
`;

const BrowseGroupListContainer = styled.div`
    background: white;
    border-radius: 8px;
    padding-right: 8px;
`;

const PlatformNode = () => {
    const isSelected = useIsPlatformSelected();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const { count } = platformAggregation;
    const registry = useEntityRegistry();
    const { trackToggleNodeEvent } = useSidebarAnalytics();

    const { icon, label } = getFilterIconAndLabel(
        PLATFORM_FILTER_NAME,
        platformAggregation.value,
        registry,
        platformAggregation.entity ?? null,
        16,
    );

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'platform'),
    });

    const onClickHeader = () => {
        if (count) toggle();
    };

    const { error, groups, loaded, observable, path, refetch } = useBrowsePagination({ skip: !isOpen });

    const color = '#000';

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={onClickHeader} style={{ paddingTop: 8 }}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={!!platformAggregation.count}
                            onClick={onClickHeader}
                            dataTestId={`browse-platform-${label}`}
                        />
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <ExpandableNode.Title color={color} size={14}>
                            {label}
                        </ExpandableNode.Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    <BrowseGroupListContainer>
                        {groups.map((group) => (
                            <BrowseProvider
                                key={group.name}
                                entityAggregation={entityAggregation}
                                environmentAggregation={environmentAggregation}
                                platformAggregation={platformAggregation}
                                browseResultGroup={group}
                                parentPath={path}
                            >
                                <BrowseNode />
                            </BrowseProvider>
                        ))}
                        {error && <SidebarLoadingError onClickRetry={refetch} />}
                        {observable}
                    </BrowseGroupListContainer>
                </ExpandableNode.Body>
            }
        />
    );
};

export default PlatformNode;
