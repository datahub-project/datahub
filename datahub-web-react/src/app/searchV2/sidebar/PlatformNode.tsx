import { Loader, Tooltip } from '@components';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import React from 'react';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
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
import BrowseRowIcon, { BROWSE_ROW_LOGO_SIZE } from '@app/searchV2/sidebar/BrowseRowIcon';
import { useHasFilterField } from '@app/searchV2/sidebar/SidebarContext';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/searchV2/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { BROWSE_PATH_V2_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import useToggle from '@app/sharedV2/useToggle';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform } from '@types';

type Props = {
    /** Tree depth (0 for top-level platforms in platform browse mode). */
    level?: number;
    hasOnlyOnePlatform?: boolean;
    /** Expands the whole sidebar; clicking a platform in the collapsed rail opens it. */
    toggleCollapse?: () => void;
    /** Sidebar is collapsed to its icon rail. */
    collapsed?: boolean;
};

const PlatformNode = ({ level = 0, hasOnlyOnePlatform = false, toggleCollapse, collapsed = false }: Props) => {
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
        BROWSE_ROW_LOGO_SIZE,
    );

    const { isOpen, toggle } = useToggle({
        initialValue: hasOnlyOnePlatform || isPlatformAndPathSelected,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'platform'),
    });

    const { error, groups, loading, observable, path, retry } = useBrowsePagination({ skip: !isOpen });

    const onSelect = () => {
        toggleCollapse?.();
        const isNowPlatformOnlySelected = !isPlatformOnlySelected;
        onSelectBrowsePath(isNowPlatformOnlySelected, [BROWSE_PATH_V2_FILTER_NAME]);
        trackSelectNodeEvent(isNowPlatformOnlySelected ? 'select' : 'deselect', 'platform');
    };

    const row = (
        <HierarchicalBrowseTreeRow
            level={level}
            isCollapsed={collapsed}
            isSelected={isPlatformOnlySelected}
            hasChildren={!!count}
            isExpanded={isOpen}
            count={count}
            icon={
                <BrowseRowIcon>
                    <DocumentSourceLogo
                        platform={platformAggregation.entity as DataPlatform}
                        size={BROWSE_ROW_LOGO_SIZE}
                        fallback={<Database size={TREE_ROW_ENTITY_ICON_SIZE} />}
                    />
                </BrowseRowIcon>
            }
            label={label}
            labelTitle={label}
            onSelect={onSelect}
            onToggleExpand={toggle}
            data-testid={`browse-platform-row-${label}`}
            expandButtonTestId={`browse-platform-${label}`}
        />
    );

    return (
        <>
            {collapsed ? (
                <Tooltip title={label} placement="right" showArrow={false}>
                    {row}
                </Tooltip>
            ) : (
                row
            )}
            {!collapsed && isOpen && (
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
                            <BrowseNode level={level + 1} />
                        </BrowseProvider>
                    ))}
                    {loading && <Loader size="sm" />}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                    {observable}
                </>
            )}
        </>
    );
};

export default PlatformNode;
