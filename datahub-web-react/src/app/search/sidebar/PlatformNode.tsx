import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { getFilterIconAndLabel } from '@app/search/filters/utils';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsPlatformSelected,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
} from '@app/search/sidebar/BrowseContext';
import BrowseNode from '@app/search/sidebar/BrowseNode';
import ExpandableNode from '@app/search/sidebar/ExpandableNode';
import { useHasFilterField } from '@app/search/sidebar/SidebarContext';
import SidebarLoadingError from '@app/search/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/search/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/search/sidebar/useSidebarAnalytics';
import { SortBy, useSort } from '@app/search/sidebar/useSort';
import { BROWSE_PATH_V2_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/search/utils/constants';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';
import { useEntityRegistry } from '@app/useEntityRegistry';

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

interface EntityNodeProps {
    sortBy: string;
}

const PlatformNode: React.FC<EntityNodeProps> = ({ sortBy }) => {
    const isPlatformSelected = useIsPlatformSelected();
    const hasBrowseFilter = useHasFilterField(BROWSE_PATH_V2_FILTER_NAME);
    const isPlatformAndPathSelected = isPlatformSelected && hasBrowseFilter;
    const isPlatformOnlySelected = isPlatformSelected && !hasBrowseFilter;
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
        initialValue: isPlatformAndPathSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'platform'),
    });

    const onClickTriangle = () => {
        if (count) toggle();
    };

    const onClickHeader = () => {
        const isNowPlatformOnlySelected = !isPlatformOnlySelected;
        onSelectBrowsePath(isNowPlatformOnlySelected, [BROWSE_PATH_V2_FILTER_NAME]);
        trackSelectNodeEvent(isNowPlatformOnlySelected ? 'select' : 'deselect', 'platform');
    };

    const { error, groups, loaded, observable, path, retry } = useBrowsePagination({ skip: !isOpen });

    const color = '#000';

    const sortedGroups = useSort(groups, sortBy as SortBy);
    console.log({ groups, sortedGroups });

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.SelectableHeader
                    isOpen={isOpen}
                    isSelected={isPlatformOnlySelected}
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
                    {sortedGroups.map((group) => (
                        <BrowseProvider
                            key={group.name}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                            browseResultGroup={group}
                            parentPath={path}
                        >
                            <BrowseNode sortBy={sortBy} />
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
