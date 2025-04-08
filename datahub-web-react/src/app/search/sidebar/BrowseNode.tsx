import { FolderOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    BrowseProvider,
    useBrowseDisplayName,
    useBrowsePathLength,
    useBrowseResultGroup,
    useEntityAggregation,
    useIsBrowsePathPrefix,
    useIsBrowsePathSelected,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
} from '@app/search/sidebar/BrowseContext';
import EntityLink from '@app/search/sidebar/EntityLink';
import ExpandableNode from '@app/search/sidebar/ExpandableNode';
import SidebarLoadingError from '@app/search/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/search/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/search/sidebar/useSidebarAnalytics';
import { SortBy, useSort } from '@app/search/sidebar/useSort';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';

import { EntityType } from '@types';

const FolderStyled = styled(FolderOutlined)`
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
    padding-right: 2px;
`;

interface EntityNodeProps {
    sortBy: string;
}

const BrowseNode: React.FC<EntityNodeProps> = ({ sortBy }) => {
    const isBrowsePathPrefix = useIsBrowsePathPrefix();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const onSelectBrowsePath = useOnSelectBrowsePath();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const browseResultGroup = useBrowseResultGroup();
    const { count, entity } = browseResultGroup;
    const hasEntityLink = !!entity && entity.type !== EntityType.DataPlatformInstance;
    const displayName = useBrowseDisplayName();
    const { trackSelectNodeEvent, trackToggleNodeEvent } = useSidebarAnalytics();

    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isBrowsePathPrefix && !isBrowsePathSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'browse'),
    });

    const onClickTriangle = () => {
        if (count) toggle();
    };

    const onClickBrowseHeader = () => {
        const isNowSelected = !isBrowsePathSelected;
        onSelectBrowsePath(isNowSelected);
        trackSelectNodeEvent(isNowSelected ? 'select' : 'deselect', 'browse');
    };

    const { error, groups, loaded, observable, path, retry } = useBrowsePagination({
        skip: !isOpen || !browseResultGroup.hasSubGroups,
    });

    const browsePathLength = useBrowsePathLength();

    const color = '#000';

    const sortedGroups = useSort(groups, sortBy as SortBy);

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.SelectableHeader
                    isOpen={isOpen}
                    isSelected={isBrowsePathSelected}
                    onClick={onClickBrowseHeader}
                    data-testid={`browse-node-${displayName}`}
                >
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={browseResultGroup.hasSubGroups}
                            onClick={onClickTriangle}
                            dataTestId={`browse-node-expand-${displayName}`}
                        />
                        <FolderStyled />
                        <ExpandableNode.Title
                            color={color}
                            size={14}
                            depth={browsePathLength}
                            maxWidth={hasEntityLink ? 175 : 200}
                            padLeft
                        >
                            {displayName}
                        </ExpandableNode.Title>
                        {hasEntityLink && <EntityLink entity={entity} targetNode="browse" />}
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(count)}</Count>
                </ExpandableNode.SelectableHeader>
            }
            body={
                <ExpandableNode.Body>
                    {sortedGroups.map((group) => (
                        <BrowseProvider
                            key={group.name}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                            browseResultGroup={group}
                            parentPath={path}
                        >
                            <BrowseNode sortBy={sortBy} />
                        </BrowseProvider>
                    ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                    {observable}
                </ExpandableNode.Body>
            }
        />
    );
};

export default BrowseNode;
