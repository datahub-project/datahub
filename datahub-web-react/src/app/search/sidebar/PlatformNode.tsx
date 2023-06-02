import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getFilterIconAndLabel } from '../filters/utils';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import useBrowsePagination from './useBrowsePagination';
import BrowseNode from './BrowseNode';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';
import {
    BrowseProvider,
    useEntityAggregation,
    useIsPlatformSelected,
    useMaybeEnvironmentAggregation,
    usePlatformAggregation,
} from './BrowseContext';

const PlatformIconContainer = styled.div`
    width: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
`;
const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const BrowseGroupListContainer = styled.div`
    background: white;
    border-radius: 8px;
    padding-bottom: 8px;
    padding-right: 8px;
`;

const PlatformNode = () => {
    const isSelected = useIsPlatformSelected();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const registry = useEntityRegistry();

    const { icon, label } = getFilterIconAndLabel(
        PLATFORM_FILTER_NAME,
        platformAggregation.value,
        registry,
        platformAggregation.entity ?? null,
        16,
    );

    const { isOpen, toggle } = useToggle(isSelected);
    const skip = !isOpen;
    const color = ANTD_GRAY[9];

    const { error, groups, loaded, observable, path, refetch } = useBrowsePagination({ skip });

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} showBorder onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen}
                            isVisible={!!platformAggregation.count}
                            onClick={toggle}
                        />
                        <PlatformIconContainer>{icon}</PlatformIconContainer>
                        <ExpandableNode.Title color={color} size={14}>
                            {label}
                        </ExpandableNode.Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    <BrowseGroupListContainer>
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
                    </BrowseGroupListContainer>
                </ExpandableNode.Body>
            }
        />
    );
};

export default PlatformNode;
