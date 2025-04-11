import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
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
import PlatformIcon from '../../sharedV2/icons/PlatformIcon';
import { DataPlatform } from '../../../types.generated';

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
