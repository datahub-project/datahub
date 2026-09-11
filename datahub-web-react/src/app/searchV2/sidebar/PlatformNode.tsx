import { Loader } from '@components';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsPlatformSelected,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
} from '@app/searchV2/sidebar/BrowseContext';
import BrowseNode from '@app/searchV2/sidebar/BrowseNode';
import { useHasFilterField } from '@app/searchV2/sidebar/SidebarContext';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/searchV2/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { BROWSE_PATH_V2_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import useToggle from '@app/shared/useToggle';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform } from '@types';

const LoadingWrapper = styled.div<{ $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
`;

const PLATFORM_ICON_STYLES = {
    backgroundColor: 'transparent',
    padding: '0px',
    borderRadius: '0px',
};

const PLATFORM_LOGO_SIZE = 16;

type Props = {
    level?: number;
    hasOnlyOnePlatform?: boolean;
    toggleCollapse?: () => void;
    collapsed?: boolean;
};

const PlatformNode = ({ level = 0, hasOnlyOnePlatform = false, toggleCollapse, collapsed = false }: Props) => {
    const theme = useTheme();
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
        TREE_ROW_ENTITY_ICON_SIZE,
    );

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: hasOnlyOnePlatform || isPlatformAndPathSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'platform'),
    });

    const onToggleExpand = () => {
        if (count) toggle();
    };

    const onSelect = () => {
        if (toggleCollapse) toggleCollapse();
        const isNowPlatformOnlySelected = !isPlatformOnlySelected;
        onSelectBrowsePath(isNowPlatformOnlySelected, [BROWSE_PATH_V2_FILTER_NAME]);
        trackSelectNodeEvent(isNowPlatformOnlySelected ? 'select' : 'deselect', 'platform');
    };

    const { error, groups, loading, loaded, observable, path, retry } = useBrowsePagination({ skip: !isOpen });

    const showChildren = !collapsed && isOpen && !isClosing && loaded;
    const childLevel = level + 1;

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={level}
                isSelected={isPlatformOnlySelected}
                isCollapsed={collapsed}
                hasChildren={!!count}
                isExpanded={isOpen && !isClosing}
                isLoadingChildren={isOpen && !loaded}
                count={count}
                icon={
                    <PlatformIcon
                        platform={platformAggregation.entity as DataPlatform}
                        size={PLATFORM_LOGO_SIZE}
                        color={theme.colors.icon}
                        styles={PLATFORM_ICON_STYLES}
                    />
                }
                label={label}
                labelTitle={label}
                onSelect={onSelect}
                onToggleExpand={collapsed ? undefined : onToggleExpand}
                expandTestId={`browse-platform-${label}`}
            />
            {showChildren && (
                <>
                    {groups.map((group) => (
                        <BrowseProvider
                            key={group.name}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                            browseResultGroup={group}
                            parentPath={path}
                        >
                            <BrowseNode level={childLevel} />
                        </BrowseProvider>
                    ))}
                    {loading && (
                        <LoadingWrapper $level={childLevel}>
                            <Loader size="xs" padding={0} />
                        </LoadingWrapper>
                    )}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                    {observable}
                </>
            )}
        </>
    );
};

export default PlatformNode;
