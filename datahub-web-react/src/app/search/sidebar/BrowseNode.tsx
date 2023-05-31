import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { FolderOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import useBrowsePagination from './useBrowsePagination';
import SidebarLoadingError from './SidebarLoadingError';
import useToggle from '../../shared/useToggle';
import {
    BrowseProvider,
    useBrowseResultGroup,
    useEntityAggregation,
    useMaybeEnvironmentAggregation,
    usePlatformAggregation,
} from './BrowseContext';

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const FolderStyled = styled(FolderOutlined)`
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const BrowseNode = () => {
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const browseResultGroup = useBrowseResultGroup();
    const { isOpen, toggle } = useToggle();
    const skip = !isOpen || !browseResultGroup.hasSubGroups;
    const color = ANTD_GRAY[9];

    // todo - set the selected browse path (useBrowsePath) in a global context
    const [isSelected, setIsSelected] = useState(false);
    const onClickHeader = () => setIsSelected((s) => !s);

    const { error, groups, loaded, observable, path, refetch } = useBrowsePagination({ skip });

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.SelectableHeader isOpen={isOpen} isSelected={isSelected} onClick={onClickHeader}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.TriangleButton
                            isOpen={isOpen}
                            isVisible={browseResultGroup.hasSubGroups}
                            onClick={toggle}
                        />
                        <FolderStyled />
                        <Title color={color}>{browseResultGroup.name}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(browseResultGroup.count)}</Count>
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
