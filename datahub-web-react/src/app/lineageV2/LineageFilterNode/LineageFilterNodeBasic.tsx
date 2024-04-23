import React, { useMemo } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';
import { getFilterIconAndLabel } from '../../searchV2/filters/utils';
import { ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME } from '../../searchV2/utils/constants';
import { useEntityRegistryV2 } from '../../useEntityRegistry';
import { isUrnTransformational, LineageFilter } from '../common';
import { useAvoidIntersectionsOften } from '../LineageEntityNode/useAvoidIntersections';
import { LINEAGE_NODE_WIDTH } from '../LineageEntityNode/useDisplayedColumns';
import useFetchFilterNodeContents, { PlatformAggregate, SubtypeAggregate } from './useFetchFilterNodeContents';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';
import { ShowMoreButton } from './ShowMoreButton';

export const LINEAGE_FILTER_NODE_NAME = 'lineage-filter';

const NodeWrapper = styled.div`
    background-color: white;
    border: 1px solid ${LINEAGE_COLORS.NODE_BORDER};
    border-radius: 12px;
    cursor: auto;
    padding: 8px;
    width: ${LINEAGE_NODE_WIDTH}px;
`;

const ExtraCard = styled.div<{ bottom: number }>`
    background-color: white;
    border: 1px solid #eee;
    border-radius: 12px;
    bottom: ${({ bottom }) => bottom}px;
    height: 40px;
    left: 0;
    position: absolute;
    width: ${LINEAGE_NODE_WIDTH}px;
    z-index: ${({ bottom }) => bottom};
`;

const TitleWrapper = styled.div`
    align-items: center;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    height: 20px;
`;

const Title = styled.div`
    flex-shrink: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const TitleCount = styled.span`
    font-weight: bold;
    margin: 0 1px;
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: 0px; top: 50%;' : 'right: 0; top: 50%;')}
`;

const PillsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 6px;
`;

const PillColumn = styled.div`
    display: flex;
    flex-direction: row;
`;

export default function LineageFilterNode(props: NodeProps<LineageFilter>) {
    const { id, data } = props;
    const { contents, shown, numShown, limit } = data;

    const entityRegistry = useEntityRegistryV2();
    const nonTransformationalContents = useMemo(
        () => contents.filter((urn) => !isUrnTransformational(urn, entityRegistry)),
        [contents, entityRegistry],
    );

    const { platforms, subtypes } = useFetchFilterNodeContents(nonTransformationalContents);

    useAvoidIntersectionsOften(id, 52);

    return (
        <NodeWrapper className="nodrag">
            <ExtraCard className="extra-card" bottom={-3} />
            <ExtraCard className="extra-card" bottom={-6} />
            <CustomHandle type="target" position={Position.Left} isConnectable={false} />
            <CustomHandle type="source" position={Position.Right} isConnectable={false} />
            <TitleWrapper>
                <Title>
                    <TitleCount>{numShown || shown.size}</TitleCount> of{' '}
                    <TitleCount>{nonTransformationalContents.length}</TitleCount> shown
                </Title>
                {limit !== nonTransformationalContents.length && <ShowMoreButton id={id} data={data} />}
            </TitleWrapper>
            <PillsWrapper>
                <PillColumn>
                    {platforms?.map((agg, index) => (
                        <PlatformEntry agg={agg} key={agg[0]} index={index} />
                    ))}
                </PillColumn>
                <PillColumn>
                    {subtypes
                        ?.filter(([filterValue]) => !filterValue.toLocaleLowerCase().endsWith('query'))
                        .map((agg, index) => (
                            <SubtypeEntry agg={agg} key={agg[0]} index={index} />
                        ))}
                </PillColumn>
            </PillsWrapper>
        </NodeWrapper>
    );
}

interface EntryProps<T> {
    agg: T;
    index: number;
}

function PlatformEntry({ agg, index }: EntryProps<PlatformAggregate>) {
    return LineageFilterEntry(PLATFORM_FILTER_NAME, agg, index);
}

function SubtypeEntry({ agg, index }: EntryProps<SubtypeAggregate>) {
    return LineageFilterEntry(ENTITY_SUB_TYPE_FILTER_NAME, agg, index);
}

const EntryWrapper = styled.span<{ includeBefore: boolean }>`
    align-items: center;
    display: flex;

    ${({ includeBefore }) =>
        includeBefore &&
        `::before {
             content: ',';
             margin-right: 4px;
         }`})
`;

const CountWrapper = styled.span`
    margin-left: 4px;
`;

function LineageFilterEntry(
    filterName: string,
    [filterValue, count, entity]: PlatformAggregate | SubtypeAggregate,
    index: number,
) {
    const entityRegistry = useEntityRegistryV2();
    const { icon, label } = getFilterIconAndLabel(filterName, filterValue, entityRegistry, entity || null, 12);

    return (
        <EntryWrapper title={label} includeBefore={index > 0}>
            {icon}
            <CountWrapper>{count}</CountWrapper>
        </EntryWrapper>
    );
}
