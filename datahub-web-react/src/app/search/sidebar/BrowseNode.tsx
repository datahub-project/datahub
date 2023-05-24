import React, { CSSProperties } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
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

const FolderStyled = styled(FolderOutlined)`
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata;
    browseResultGroup: BrowseResultGroupV2;
    path: Array<string>;
};

const BrowseNode = ({
    entityAggregation,
    environmentAggregation,
    platformAggregation,
    browseResultGroup,
    path,
}: Props) => {
    const entityType = entityAggregation.value as EntityType;
    const environment = environmentAggregation?.value;
    const platform = platformAggregation.value;

    const { isOpen, toggle } = useToggle();

    const { loaded, error, groups, pathResult } = useBrowseV2Query({
        skip: !isOpen,
        entityType,
        environment,
        platform,
        path,
    });

    const color = ANTD_GRAY[9];
    const iconProps: CSSProperties = { visibility: browseResultGroup.hasSubGroups ? 'visible' : 'hidden' };

    return (
        <ExpandableNode
            isOpen={isOpen && loaded}
            header={
                <ExpandableNode.Header isOpen={isOpen} onClick={toggle}>
                    <ExpandableNode.HeaderLeft>
                        <ExpandableNode.Triangle isOpen={isOpen} style={iconProps} />
                        <FolderStyled />
                        <Title color={color}>{browseResultGroup.name}</Title>
                    </ExpandableNode.HeaderLeft>
                    <Count color={color}>{formatNumber(platformAggregation.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                    {groups.map((group) => (
                        <BrowseNode
                            key={group.name}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                            browseResultGroup={group}
                            path={[...pathResult, group.name]}
                        />
                    ))}
                </ExpandableNode.Body>
            }
        />
    );
};

export default BrowseNode;
