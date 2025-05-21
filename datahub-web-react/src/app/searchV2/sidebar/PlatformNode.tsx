import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsPlatformBrowseMode,
    useIsPlatformSelected,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
} from '@app/searchV2/sidebar/BrowseContext';
import BrowseNode from '@app/searchV2/sidebar/BrowseNode';
import ExpandableNode from '@app/searchV2/sidebar/ExpandableNode';
import { useHasFilterField } from '@app/searchV2/sidebar/SidebarContext';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/searchV2/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { BROWSE_PATH_V2_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform } from '@types';

const Count = styled(Typography.Text)<{ $isPlatformBrowse: boolean; isOpen: boolean }>`
    font-size: 10px;
    color: ${(props) => props.color};
    padding: 2px 8px;
    margin-left: 8px;
    ${(props) => props.$isPlatformBrowse && `border-radius: 8px;`}
    ${(props) => props.$isPlatformBrowse && `background-color: ${props.isOpen ? '#fff' : ANTD_GRAY[3]};`}
`;

type Props = {
    iconSize?: number;
    hasOnlyOnePlatform?: boolean;
    toggleCollapse?: () => void;
    collapsed?: boolean;
};

const PlatformNode = ({ iconSize = 20, hasOnlyOnePlatform = false, toggleCollapse, collapsed = true }: Props) => {
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

    const { label } = getFilterIconAndLabel(
        PLATFORM_FILTER_NAME,
        platformAggregation.value,
        registry,
        platformAggregation.entity ?? null,
        iconSize,
    );

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: hasOnlyOnePlatform || isPlatformAndPathSelected,
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

    const color = REDESIGN_COLORS.TEXT_HEADING;

    const nodeStyle = {
        padding: '4px',
        margin: '0px',
        background: collapsed ? '#fff' : isOpen && REDESIGN_COLORS.BACKGROUND_GRAY_4,
        borderLeft: isOpen && '1px solid #fff',
        borderRight: isOpen && '1px solid #fff',
        justifyContent: collapsed ? 'center' : 'start',
        display: 'flex',
        flexDirection: 'column',
    };

    const onClick = () => {
        if (toggleCollapse) toggleCollapse();
        onClickHeader();
    };

    return (
        <ExpandableNode
            isOpen={!collapsed && isOpen && !isClosing && loaded}
            style={(isPlatformBrowse && { ...nodeStyle }) || undefined}
            header={
                <ExpandableNode.SelectableHeader
                    isOpen={isOpen}
                    $isSelected={isPlatformOnlySelected}
                    showBorder
                    onClick={onClick}
                    style={{ justifyContent: collapsed ? 'center' : 'start' }}
                >
                    <ExpandableNode.HeaderLeft style={{ justifyContent: collapsed ? 'center' : 'start' }}>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={!!platformAggregation.count}
                            onClick={onClickTriangle}
                            dataTestId={`browse-platform-${label}`}
                            style={{
                                marginRight: 4,
                                transform: isOpen && 'rotate(90deg)',
                                transition: 'transform 200ms ease',
                                ...(collapsed && { display: 'none' }),
                            }}
                        />
                        <PlatformIcon
                            platform={platformAggregation.entity as DataPlatform}
                            size={iconSize}
                            color={REDESIGN_COLORS.BORDER_1}
                        />
                        {!collapsed && (
                            <ExpandableNode.Title
                                color={color}
                                size={isPlatformBrowse ? 14 : 12}
                                maxWidth={125}
                                padLeft
                            >
                                {label}
                            </ExpandableNode.Title>
                        )}
                        {!collapsed && isPlatformBrowse && (
                            <Count color={color} isOpen={isOpen} $isPlatformBrowse>
                                {formatNumber(platformAggregation.count)}
                            </Count>
                        )}
                    </ExpandableNode.HeaderLeft>
                    {!collapsed && !isPlatformBrowse && (
                        <Count color={color} isOpen={isOpen} $isPlatformBrowse={false}>
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
