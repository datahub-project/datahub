import React, { CSSProperties } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { VscTriangleDown, VscTriangleRight } from 'react-icons/vsc';
import { FolderOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';
import { AggregationMetadata, BrowseResultGroupV2, EntityType } from '../../../types.generated';
import useBrowseV2Query from './useBrowseV2Query';
import useToggle from './useToggle';

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const path = [];

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata;
    browseResultGroup: BrowseResultGroupV2;
};

const BrowseNode = ({ entityAggregation, environmentAggregation, platformAggregation, browseResultGroup }: Props) => {
    const entityType = entityAggregation.value as EntityType;
    const environment = environmentAggregation?.value;
    const platform = platformAggregation.value;

    const { isOpen, toggle } = useToggle();

    const { loaded, error, groups } = useBrowseV2Query({
        skip: !isOpen,
        entityType,
        environment,
        platform,
        path,
    });

    const color = ANTD_GRAY[9];
    const iconProps: CSSProperties = { color, visibility: browseResultGroup.hasSubGroups ? 'visible' : 'hidden' };

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        {isOpen ? <VscTriangleDown style={iconProps} /> : <VscTriangleRight style={iconProps} />}
                        <FolderOutlined style={{ fontSize: 16 }} />
                        <Title color={color}>{browseResultGroup.name}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {groups?.map((group) => (
                        <BrowseNode
                            key={group.name}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                            browseResultGroup={group}
                        />
                    ))}
                </ExpandableNode.Body>
            }
        />
    );
};

export default BrowseNode;
