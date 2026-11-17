import { Loader } from '@components';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import React from 'react';

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
import BrowseRowIcon from '@app/searchV2/sidebar/BrowseRowIcon';
import EntityLink from '@app/searchV2/sidebar/EntityLink';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/searchV2/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import useToggle from '@app/sharedV2/useToggle';

import { EntityType } from '@types';

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

    const { isOpen, toggle } = useToggle({
        initialValue: isBrowsePathPrefix && !isBrowsePathSelected,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'browse'),
    });

    const onSelect = () => {
        const isNowSelected = !isBrowsePathSelected;
        onSelectBrowsePath(isNowSelected);
        trackSelectNodeEvent(isNowSelected ? 'select' : 'deselect', 'browse');
    };

    const { error, groups, loading, observable, path, retry } = useBrowsePagination({
        skip: !isOpen || !hasSubGroups,
    });

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={level}
                isSelected={isBrowsePathSelected}
                hasChildren={hasSubGroups}
                isExpanded={isOpen}
                count={count}
                icon={
                    <BrowseRowIcon $isSelected={isBrowsePathSelected}>
                        <Folder size={TREE_ROW_ENTITY_ICON_SIZE} weight={isBrowsePathSelected ? 'fill' : 'regular'} />
                    </BrowseRowIcon>
                }
                label={displayName}
                labelTitle={displayName}
                trailing={hasEntityLink ? <EntityLink entity={entity} targetNode="browse" /> : undefined}
                onSelect={onSelect}
                onToggleExpand={hasSubGroups ? toggle : undefined}
                data-testid={`browse-node-${displayName}`}
                expandButtonTestId={`browse-node-expand-${displayName}`}
            />
            {isOpen && hasSubGroups && (
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

export default BrowseNode;
