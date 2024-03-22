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
    useIsPlatformBrowseMode,
} from './BrowseContext';
import useSidebarAnalytics from './useSidebarAnalytics';
import { useHasFilterField } from './SidebarContext';
import { ANTD_GRAY } from '../../entity/shared/constants';

const PlatformIconContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    margin-right: 8px;
`;

const Count = styled(Typography.Text)<{ isPlatformBrowse: boolean }>`
    font-size: 12px;
    color: ${(props) => props.color};
    padding: 2px 8px;
    margin-left: 8px;
    ${(props) => props.isPlatformBrowse && `border-radius: 12px;`}
    ${(props) => props.isPlatformBrowse && `background-color: ${ANTD_GRAY[3]};`}
`;

const PlatformNode = () => {
    const isPlatformBrowse = useIsPlatformBrowseMode();
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

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            style={(isPlatformBrowse && { margin: '12px 12px' }) || undefined}
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
                            style={{ marginRight: 4 }}
                        />
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <ExpandableNode.Title color={color} size={isPlatformBrowse ? 16 : 14} padLeft>
                            {label}
                        </ExpandableNode.Title>
                        {isPlatformBrowse && (
                            <Count color={color} isPlatformBrowse>
                                {formatNumber(platformAggregation.count)}
                            </Count>
                        )}
                    </ExpandableNode.HeaderLeft>
                    {!isPlatformBrowse && (
                        <Count color={color} isPlatformBrowse={false}>
                            {formatNumber(platformAggregation.count)}
                        </Count>
                    )}
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
