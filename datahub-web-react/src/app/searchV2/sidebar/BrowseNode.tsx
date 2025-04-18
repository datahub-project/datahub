import { FolderOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
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
import ExpandableNode from '@app/searchV2/sidebar/ExpandableNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useBrowsePagination from '@app/searchV2/sidebar/useBrowsePagination';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { formatNumber } from '@app/shared/formatNumber';
import useToggle from '@app/shared/useToggle';

import { EntityType } from '@types';

const FolderStyled = styled(FolderOutlined)`
    font-size: 16px;
    color: #374066;
    margin-right: 4px;
`;

const Count = styled(Typography.Text)`
    font-size: 10px;
    color: ${(props) => props.color};
    padding: 2px 8px;
    margin-left: 8px;
    border-radius: 12px;
    background-color: ${ANTD_GRAY[1]};
    display: block;
    flex-grow: 0;
`;

const BrowseNode = () => {
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

    const color = '#374066';

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.SelectableHeader
                    isOpen={isOpen}
                    $isSelected={isBrowsePathSelected}
                    onClick={onClickBrowseHeader}
                    data-testid={`browse-node-${displayName}`}
                >
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={browseResultGroup.hasSubGroups}
                            onClick={onClickTriangle}
                            dataTestId={`browse-node-expand-${displayName}`}
                            style={{ display: 'block', width: 18 }}
                        />
                        <FolderStyled />
                        <ExpandableNode.Title color={color} size={12} dynamicWidth padLeft>
                            {displayName}
                        </ExpandableNode.Title>
                        {hasEntityLink && <EntityLink entity={entity} targetNode="browse" />}
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(count)}</Count>
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

export default BrowseNode;
