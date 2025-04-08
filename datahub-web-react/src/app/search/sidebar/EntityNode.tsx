import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import {
    BrowseProvider,
    useEntityAggregation,
    useEntityType,
    useIsEntitySelected,
} from '@app/search/sidebar/BrowseContext';
import EnvironmentNode from '@app/search/sidebar/EnvironmentNode';
import ExpandableNode from '@app/search/sidebar/ExpandableNode';
import PlatformNode from '@app/search/sidebar/PlatformNode';
import { useHasFilterField } from '@app/search/sidebar/SidebarContext';
import SidebarLoadingError from '@app/search/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/search/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/search/sidebar/useSidebarAnalytics';
import { MAX_COUNT_VAL, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/search/utils/constants';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';
import { useEntityRegistry } from '@app/useEntityRegistry';

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
    padding-left: 4px;
`;

interface EntityNodeProps {
    sortBy: string;
}

const EntityNode: React.FC<EntityNodeProps> = ({ sortBy }) => {
    const isSelected = useIsEntitySelected();
    const entityType = useEntityType();
    const entityAggregation = useEntityAggregation();
    const hasEnvironmentFilter = useHasFilterField(ORIGIN_FILTER_NAME);
    const { count } = entityAggregation;
    const countText = count === MAX_COUNT_VAL ? '10k+' : formatNumber(count);
    const registry = useEntityRegistry();
    const { trackToggleNodeEvent } = useSidebarAnalytics();

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'entity'),
    });

    const onClickHeader = (e) => {
        e.preventDefault();
        if (count) toggle();
    };

    const { loaded, error, environmentAggregations, platformAggregations, retry } = useAggregationsQuery({
        skip: !isOpen,
        facets: [ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME],
    });

    const showEnvironments =
        environmentAggregations &&
        (environmentAggregations.length > 1 || (hasEnvironmentFilter && !!environmentAggregations.length));
    const color = count > 0 ? '#000' : ANTD_GRAY[8];

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.Header
                    isOpen={isOpen}
                    showBorder
                    onClick={onClickHeader}
                    style={{ paddingTop: '16px' }}
                    data-testid={`browse-entity-${registry.getCollectionName(entityType)}`}
                >
                    <ExpandableNode.HeaderLeft>
                        {registry.getIcon(entityType, 16, IconStyleType.HIGHLIGHT, color)}
                        <ExpandableNode.Title color={color} size={16} padLeft>
                            {registry.getCollectionName(entityType)}
                        </ExpandableNode.Title>
                        <Count color={color}>{countText}</Count>
                    </ExpandableNode.HeaderLeft>
                    <ExpandableNode.CircleButton isOpen={isOpen && !isClosing} color={color} />
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {showEnvironments
                        ? environmentAggregations?.map((environmentAggregation) => (
                              <BrowseProvider
                                  key={environmentAggregation.value}
                                  entityAggregation={entityAggregation}
                                  environmentAggregation={environmentAggregation}
                              >
                                  <EnvironmentNode sortBy={sortBy} />
                              </BrowseProvider>
                          ))
                        : platformAggregations?.map((platformAggregation) => (
                              <BrowseProvider
                                  key={platformAggregation.value}
                                  entityAggregation={entityAggregation}
                                  platformAggregation={platformAggregation}
                              >
                                  <PlatformNode sortBy={sortBy} />
                              </BrowseProvider>
                          ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </ExpandableNode.Body>
            }
        />
    );
};

export default EntityNode;
