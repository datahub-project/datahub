import React from 'react';
import styled from 'styled-components';
import { Tooltip, Typography } from 'antd';
import { FolderOutlined } from '@ant-design/icons';
import Icon from '@ant-design/icons/lib/components/Icon';
import { useHistory } from 'react-router';
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
    useBrowseDisplayName,
} from './BrowseContext';
import useSidebarAnalytics from './useSidebarAnalytics';
import { EntityType } from '../../../types.generated';
import { ReactComponent as ExternalLink } from '../../../images/link-out.svg';
import { useEntityRegistry } from '../../useEntityRegistry';

// todo - move this back into ExpandableNode if it makes sense
const SelectableHeader = styled(ExpandableNode.Header)<{ isSelected: boolean }>`
    & {
        border: 1px solid ${(props) => (props.isSelected ? props.theme.styles['primary-color'] : 'transparent')};
        background-color: ${(props) => (props.isSelected ? props.theme.styles['primary-color-light'] : 'transparent')};
        border-radius: 8px;
    }

    &:hover {
        background-color: ${(props) => props.theme.styles['primary-color-light']};
    }
`;

const FolderStyled = styled(FolderOutlined)`
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const ContainerIcon = styled(Icon)<{ isSelected: boolean }>`
    && {
        color: ${(props) => props.theme.styles['primary-color']};
        ${(props) => !props.isSelected && 'display: none;'}
        ${SelectableHeader}:hover & {
            display: inherit;
        }
    }
`;

const BrowseNode = () => {
    const registry = useEntityRegistry();
    const history = useHistory();
    const isBrowsePathPrefix = useIsBrowsePathPrefix();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const onSelectBrowsePath = useOnSelectBrowsePath();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const browseResultGroup = useBrowseResultGroup();
    const { trackToggleNodeEvent } = useSidebarAnalytics();
    const { count } = browseResultGroup;
    const displayName = useBrowseDisplayName();
    const { trackSelectNodeEvent } = useSidebarAnalytics();

    const isContainer = browseResultGroup.entity?.type === EntityType.Container;

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

    const onClickContainerButton = () => {
        const { entity } = browseResultGroup;
        if (!entity) return;
        const containerUrl = registry.getEntityUrl(entity.type, entity.urn);
        history.push(containerUrl);
    };

    const { error, groups, loaded, observable, path, refetch } = useBrowsePagination({
        skip: !isOpen || !browseResultGroup.hasSubGroups,
    });

    const browsePathLength = useBrowsePathLength();

    const color = '#000';

    return (
        <ExpandableNode
            isOpen={isOpen && !isClosing && loaded}
            header={
                <SelectableHeader
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
                        <ExpandableNode.Title color={color} size={14} depth={browsePathLength}>
                            {displayName} {displayName} {displayName} {displayName} {displayName} {displayName}{' '}
                            {displayName} {displayName} {displayName}{' '}
                        </ExpandableNode.Title>
                        {isContainer && (
                            <Tooltip placement="top" title={`view ${displayName} profile`}>
                                <ExpandableNode.StaticButton
                                    icon={
                                        <ContainerIcon
                                            isSelected={isBrowsePathSelected}
                                            component={ExternalLink}
                                            onClick={onClickContainerButton}
                                        />
                                    }
                                />
                            </Tooltip>
                        )}
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(987654321)}</Count>
                </SelectableHeader>
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
