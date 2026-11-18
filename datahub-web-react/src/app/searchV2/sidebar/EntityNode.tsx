import React from 'react';

import { IconStyleType } from '@app/entityV2/Entity';
import {
    BrowseProvider,
    useEntityAggregation,
    useEntityType,
    useIsEntitySelected,
} from '@app/searchV2/sidebar/BrowseContext';
import BrowseRowIcon from '@app/searchV2/sidebar/BrowseRowIcon';
import EnvironmentNode from '@app/searchV2/sidebar/EnvironmentNode';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import { useHasFilterField } from '@app/searchV2/sidebar/SidebarContext';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import useToggle from '@app/sharedV2/useToggle';
import { useEntityRegistry } from '@app/useEntityRegistry';

const EntityNode = () => {
    const isSelected = useIsEntitySelected();
    const entityType = useEntityType();
    const entityAggregation = useEntityAggregation();
    const hasEnvironmentFilter = useHasFilterField(ORIGIN_FILTER_NAME);
    const { count } = entityAggregation || { count: 0 };
    const registry = useEntityRegistry();
    const { trackToggleNodeEvent } = useSidebarAnalytics();
    const collectionName = registry.getCollectionName(entityType);

    const { isOpen, toggle } = useToggle({
        initialValue: isSelected,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'entity'),
    });

    const onToggle = () => {
        if (count) toggle();
    };

    const { error, environmentAggregations, platformAggregations, retry } = useAggregationsQuery({
        skip: !isOpen,
        facets: [ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME],
    });

    const showEnvironments =
        environmentAggregations &&
        (environmentAggregations.length > 1 || (hasEnvironmentFilter && !!environmentAggregations.length));

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={0}
                isSelected={false}
                hasChildren={count > 0}
                isExpanded={isOpen}
                count={count}
                icon={
                    <BrowseRowIcon>
                        {registry.getIcon(entityType, TREE_ROW_ENTITY_ICON_SIZE, IconStyleType.ACCENT)}
                    </BrowseRowIcon>
                }
                label={collectionName}
                labelTitle={collectionName}
                onSelect={onToggle}
                onToggleExpand={onToggle}
                data-testid={`browse-entity-${collectionName}`}
            />
            {isOpen && (
                <>
                    {showEnvironments
                        ? environmentAggregations?.map((environmentAggregation) => (
                              <BrowseProvider
                                  key={environmentAggregation.value}
                                  entityAggregation={entityAggregation}
                                  environmentAggregation={environmentAggregation}
                              >
                                  <EnvironmentNode level={1} />
                              </BrowseProvider>
                          ))
                        : platformAggregations?.map((platformAggregation) => (
                              <BrowseProvider
                                  key={platformAggregation.value}
                                  entityAggregation={entityAggregation}
                                  platformAggregation={platformAggregation}
                              >
                                  <PlatformNode level={1} />
                              </BrowseProvider>
                          ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </>
            )}
        </>
    );
};

export default EntityNode;
