import React from 'react';
import { useTheme } from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import {
    BrowseProvider,
    useEntityAggregation,
    useEntityType,
    useIsEntitySelected,
} from '@app/searchV2/sidebar/BrowseContext';
import EnvironmentNode from '@app/searchV2/sidebar/EnvironmentNode';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import { useHasFilterField } from '@app/searchV2/sidebar/SidebarContext';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import useToggle from '@app/shared/useToggle';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

const EntityNode = () => {
    const theme = useTheme();
    const isSelected = useIsEntitySelected();
    const entityType = useEntityType();
    const entityAggregation = useEntityAggregation();
    const hasEnvironmentFilter = useHasFilterField(ORIGIN_FILTER_NAME);
    const { count } = entityAggregation || { count: 0 };
    const registry = useEntityRegistry();
    const { trackToggleNodeEvent } = useSidebarAnalytics();
    const collectionName = registry.getCollectionName(entityType);

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'entity'),
    });

    const onToggle = () => {
        if (count) toggle();
    };

    const { loaded, error, environmentAggregations, platformAggregations, retry } = useAggregationsQuery({
        skip: !isOpen,
        facets: [ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME],
    });

    const showEnvironments =
        environmentAggregations &&
        (environmentAggregations.length > 1 || (hasEnvironmentFilter && !!environmentAggregations.length));
    const showChildren = isOpen && !isClosing && loaded;
    const iconColor = count > 0 ? theme.colors.icon : theme.colors.iconDisabled;

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={0}
                isSelected={isSelected}
                hasChildren={count > 0}
                isExpanded={isOpen && !isClosing}
                isLoadingChildren={isOpen && !loaded}
                count={count}
                icon={registry.getIcon(entityType, TREE_ROW_ENTITY_ICON_SIZE, IconStyleType.HIGHLIGHT, iconColor)}
                label={collectionName}
                labelTitle={collectionName}
                onSelect={onToggle}
                onToggleExpand={onToggle}
                data-testid={`browse-entity-${collectionName}`}
            />
            {showChildren && (
                <>
                    {showEnvironments
                        ? environmentAggregations?.map((environmentAggregation) => (
                              <BrowseProvider
                                  key={environmentAggregation.value}
                                  entityAggregation={entityAggregation}
                                  environmentAggregation={environmentAggregation}
                              >
                                  <EnvironmentNode />
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
