import { Loader, Pill } from '@components';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import {
    BrowseProvider,
    useBrowseDisplayName,
    useBrowseResultGroup,
    useEntityAggregation,
    useIsBrowsePathPrefix,
    useIsBrowsePathSelected,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
} from '@app/searchV2/sidebar/BrowseContext';
import EntityLink from '@app/searchV2/sidebar/EntityLink';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/searchV2/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

import { EntityType } from '@types';

const LoadingWrapper = styled.div<{ $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
`;

type Props = {
    level: number;
};

const BrowseNode = ({ level }: Props) => {
    const isBrowsePathPrefix = useIsBrowsePathPrefix();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const onSelectBrowsePath = useOnSelectBrowsePath();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const browseResultGroup = useBrowseResultGroup();
    const { count, entity, hasSubGroups } = browseResultGroup;
    const hasEntityLink = !!entity && entity.type !== EntityType.DataPlatformInstance;
    const displayName = useBrowseDisplayName();
    const { trackSelectNodeEvent, trackToggleNodeEvent } = useSidebarAnalytics();
    const theme = useTheme();

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isBrowsePathPrefix && !isBrowsePathSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'browse'),
    });

    const onToggleExpand = () => {
        if (count) toggle();
    };

    const onSelect = () => {
        const isNowSelected = !isBrowsePathSelected;
        onSelectBrowsePath(isNowSelected);
        trackSelectNodeEvent(isNowSelected ? 'select' : 'deselect', 'browse');
    };

    const { error, groups, loading, loaded, observable, path, retry } = useBrowsePagination({
        skip: !isOpen || !browseResultGroup.hasSubGroups,
    });

    const showChildren = isOpen && !isClosing && loaded && hasSubGroups;
    const childLevel = level + 1;
    const leafCount = !hasSubGroups && count > 0 ? <Pill label={formatNumber(count)} size="sm" /> : null;

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={level}
                isSelected={isBrowsePathSelected}
                hasChildren={hasSubGroups}
                isExpanded={isOpen && !isClosing}
                isLoadingChildren={isOpen && !loaded}
                count={hasSubGroups ? count : undefined}
                icon={<Folder size={TREE_ROW_ENTITY_ICON_SIZE} color={theme.colors.icon} />}
                label={displayName}
                labelTitle={displayName}
                trailing={
                    hasEntityLink || leafCount ? (
                        <>
                            {hasEntityLink ? <EntityLink entity={entity} targetNode="browse" /> : null}
                            {leafCount}
                        </>
                    ) : undefined
                }
                onSelect={onSelect}
                onToggleExpand={hasSubGroups ? onToggleExpand : undefined}
                expandTestId={`browse-node-expand-${displayName}`}
                data-testid={`browse-node-${displayName}`}
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

export default BrowseNode;
