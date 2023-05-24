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
import useIntersect from '../../shared/useIntersect';

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
    const { isOpen, toggle } = useToggle();

    const skip = !isOpen || !browseResultGroup.hasSubGroups;

    const { loaded, error, groups, pathResult, loadMore } = useBrowseV2Query({
        skip,
        entityType: entityAggregation.value as EntityType,
        environment: environmentAggregation?.value,
        platform: platformAggregation.value,
        path,
    });

    const color = ANTD_GRAY[9];
    const iconProps: CSSProperties = { visibility: browseResultGroup.hasSubGroups ? 'visible' : 'hidden' };

    // todo - what if we open a bunch of these, then expand/re-open them?
    // will that cause the intersection all at once for a bunch of them?

    // maybe we should ignore results for a bit/debounce or something after recently opening/loading
    // basically the skip/loading condition should trigger a debounced toggle that we use to enable this thing
    // disable can be right away
    //
    const { observableRef } = useIntersect({ skip, initialDelay: 500, onIntersect: loadMore });

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
                    <Count color={color}>{formatNumber(browseResultGroup.count)}</Count>
                </ExpandableNode.Header>
            }
            body={
                <ExpandableNode.Body>
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
                    <div ref={observableRef}>observable (HIDE ME)</div>
                    {error && <Typography.Text type="danger">There was a problem loading the sidebar.</Typography.Text>}
                </ExpandableNode.Body>
            }
        />
    );
};

export default BrowseNode;
