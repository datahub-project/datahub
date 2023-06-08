import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { FolderOutlined } from '@ant-design/icons';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import useBrowsePagination from './useBrowsePagination';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';
import {
    BrowseProvider,
    useBrowseResultGroup,
    useMaybeEnvironmentAggregation,
    useOnSelectBrowsePath,
    usePlatformAggregation,
    useEntityAggregation,
    useIsBrowsePathSelected,
    useIsBrowsePathPrefix,
    useBrowsePathLength,
} from './BrowseContext';
import { useEntityRegistry } from '../../useEntityRegistry';

const FolderStyled = styled(FolderOutlined)`
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const BrowseNode = () => {
    const isBrowsePathPrefix = useIsBrowsePathPrefix();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const onSelectBrowsePath = useOnSelectBrowsePath();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const browseResultGroup = useBrowseResultGroup();
    const { count } = browseResultGroup;

    const { isOpen, isClosing, toggle } = useToggle({ initialValue: isBrowsePathPrefix && !isBrowsePathSelected });

    const onClickTriangle = () => {
        if (!count) return;
        toggle();
    };

    const { error, groups, loaded, observable, path, refetch } = useBrowsePagination({
        skip: !isOpen || !browseResultGroup.hasSubGroups,
    });

    const browsePathLength = useBrowsePathLength();

    const { entity } = browseResultGroup;
    const displayedName = entity ? entityRegistry.getDisplayName(entity.type, entity) : browseResultGroup.name;

    const color = '#000';

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <ExpandableNode.SelectableHeader
                    isOpen={isOpen}
                    isSelected={isBrowsePathSelected}
                    onClick={onSelectBrowsePath}
                >
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen && !isClosing}
                            isVisible={browseResultGroup.hasSubGroups}
                            onClick={onClickTriangle}
                        />
                        <FolderStyled />
                        <ExpandableNode.Title color={color} size={14} depth={browsePathLength}>
                            {displayedName}
                        </ExpandableNode.Title>
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
                    {error && <SidebarLoadingError onClickRetry={refetch} />}
                    {observable}
                </ExpandableNode.Body>
            }
        />
    );
};

export default BrowseNode;
