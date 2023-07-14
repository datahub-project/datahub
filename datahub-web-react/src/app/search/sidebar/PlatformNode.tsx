import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getFilterIconAndLabel } from '../filters/utils';
import { BROWSE_PATH_V2_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import useBrowsePagination from './useBrowsePagination';
import BrowseNode from './BrowseNode';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsPlatformSelected,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
} from './BrowseContext';
import useSidebarAnalytics from './useSidebarAnalytics';
import { useHasFilterField } from './SidebarContext';

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

const PlatformNode = () => {
    const isPlatformSelected = useIsPlatformSelected();
    const hasBrowseFilter = useHasFilterField(BROWSE_PATH_V2_FILTER_NAME);
    const isPlatformSelectedPrefix = isPlatformSelected && hasBrowseFilter;
    const isPlatformSelectedExact = isPlatformSelected && !hasBrowseFilter;
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const { count } = platformAggregation;
    const registry = useEntityRegistry();
    const { trackToggleNodeEvent, trackSelectNodeEvent } = useSidebarAnalytics();
    const onSelectBrowsePath = useOnSelectBrowsePath();

    const { icon, label } = getFilterIconAndLabel(
        PLATFORM_FILTER_NAME,
        platformAggregation.value,
        registry,
        platformAggregation.entity ?? null,
        16,
    );

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isPlatformSelectedPrefix,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'platform'),
    });

    const onClickTriangle = () => {
        if (count) toggle();
    };

    const onClickHeader = () => {
        const isNowSelected = !isPlatformSelectedExact;
        onSelectBrowsePath(isNowSelected, [BROWSE_PATH_V2_FILTER_NAME]);
        trackSelectNodeEvent(isNowSelected ? 'select' : 'deselect', 'platform');
    };

    const { error, groups, loaded, observable, path, retry } = useBrowsePagination({ skip: !isOpen });

    const color = '#000';

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.SelectableHeader
                    isOpen={isOpen}
                    isSelected={isPlatformSelectedExact}
                    showBorder
                    onClick={onClickHeader}
                >
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={!!platformAggregation.count}
                            onClick={onClickTriangle}
                            dataTestId={`browse-platform-${label}`}
                        />
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <ExpandableNode.Title color={color} size={14} padLeft>
                            {label}
                        </ExpandableNode.Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.SelectableHeader>
            }
            body={
                <ExpandableNode.Body>
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
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                    {observable}
                </ExpandableNode.Body>
            }
        />
    );
};

export default PlatformNode;
