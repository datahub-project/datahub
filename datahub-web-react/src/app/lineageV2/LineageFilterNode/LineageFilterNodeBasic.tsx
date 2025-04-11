import { EntityType } from '@types';
import React, { useContext, useState } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';
import { getFilterIconAndLabel } from '../../searchV2/filters/utils';
import { ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME } from '../../searchV2/utils/constants';
import { useEntityRegistryV2 } from '../../useEntityRegistry';
import { LineageFilter, LineageNodesContext, useIgnoreSchemaFieldStatus } from '../common';
import { useAvoidIntersectionsOften } from '../LineageEntityNode/useAvoidIntersections';
import { LINEAGE_NODE_WIDTH } from '../LineageEntityNode/useDisplayedColumns';
import { ShowMoreButton } from './ShowMoreButton';
import useFetchFilterNodeContents, { PlatformAggregate, SubtypeAggregate } from './useFetchFilterNodeContents';
import LineageFilterSearch from './LineageFilterSearch';

export const LINEAGE_FILTER_NODE_NAME = 'lineage-filter';

const NodeWrapper = styled.div`
    background-color: white;
    border: 1px solid ${LINEAGE_COLORS.NODE_BORDER};
    border-radius: 12px;
    cursor: pointer;
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
    const { parent, direction, shown, allChildren, numShown } = data;

    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const { showGhostEntities, rootType } = useContext(LineageNodesContext);
    const { total, platforms, subtypes } = useFetchFilterNodeContents(
        parent,
        direction,
        showGhostEntities && ignoreSchemaFieldStatus && rootType === EntityType.SchemaField,
    );

    // If a user has searched, result list may be limited to the number of matches
    const [numMatches, setNumMatches] = useState<number>(0);

    useAvoidIntersectionsOften(id, numMatches ? 133 : 117);

    const numerator = numShown ?? shown.size;
    const denominator = showGhostEntities ? allChildren.size : total ?? allChildren.size;
    return (
        <NodeWrapper>
            <ExtraCard className="extra-card" bottom={-3} />
            <ExtraCard className="extra-card" bottom={-6} />
            <CustomHandle type="target" position={Position.Left} isConnectable={false} />
            <CustomHandle type="source" position={Position.Right} isConnectable={false} />
            <TitleWrapper>
                <Title>
                    <TitleCount>{Math.min(numerator, denominator)}</TitleCount> of{' '}
                    <TitleCount>
                        {denominator}
                        {showGhostEntities && '+'}
                    </TitleCount>{' '}
                    shown
                </Title>
                <ShowMoreButton data={data} numMatches={numMatches} />
            </TitleWrapper>
            <LineageFilterSearch data={data} numMatches={numMatches} setNumMatches={setNumMatches} />
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
