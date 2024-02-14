import React from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';
import { LINEAGE_NODE_WIDTH } from '../LineageEntityNode/useDisplayedColumns';
import useFetchFilterNodeContents from './useFetchFilterNodeContents';
import { LINEAGE_COLORS, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { PlatformEntry, SubtypeEntry } from './LineageFilterPills';
import { LineageFilter } from '../common';
import { ShowMoreButton } from './ShowMoreButton';

export const LINEAGE_FILTER_NODE_NAME = 'lineage-filter';

const PILL_COLUMN_MAX = 3;

const NodeWrapper = styled.div`
    background-color: white;
    border: 1px solid #eee;
    border-radius: 12px;
    cursor: auto;
    padding: 8px;
    width: ${LINEAGE_NODE_WIDTH}px;

    :hover:not(:has(.pill-true:hover, .show-more:hover)) {
        border-color: ${REDESIGN_COLORS.BLUE}80;
        .extra-card {
            border-color: ${REDESIGN_COLORS.BLUE}80;
        }
    }
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
`;

const Title = styled.div`
    color: ${LINEAGE_COLORS.BLUE_1};
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
    flex-direction: row;
    margin-top: 6px;
`;

const PillColumn = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    width: 50%;
`;

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid;
    opacity: 0.1;
    vertical-align: text-top;
`;

export default function LineageFilterNode(props: NodeProps<LineageFilter>) {
    const { id, data } = props;
    const { contents, shown } = data;

    const { platforms, subtypes } = useFetchFilterNodeContents(contents);
    return (
        <NodeWrapper className="nodrag">
            <ExtraCard className="extra-card" bottom={-3} />
            <ExtraCard className="extra-card" bottom={-6} />
            <CustomHandle type="target" position={Position.Left} isConnectable={false} />
            <CustomHandle type="source" position={Position.Right} isConnectable={false} />
            <TitleWrapper>
                <Title>
                    <TitleCount>{shown.size}</TitleCount> of <TitleCount>{contents.length}</TitleCount> shown
                </Title>
                {shown.size !== contents.length && <ShowMoreButton id={id} data={data} />}
            </TitleWrapper>
            <PillsWrapper>
                <PillColumn>
                    {platforms?.slice(0, PILL_COLUMN_MAX).map((agg) => (
                        <PlatformEntry agg={agg} key={agg[0]} data={data} />
                    ))}
                </PillColumn>
                <VerticalDivider margin={4} />
                <PillColumn>
                    {subtypes?.slice(0, PILL_COLUMN_MAX).map((agg) => (
                        <SubtypeEntry agg={agg} key={agg[0]} data={data} />
                    ))}
                </PillColumn>
            </PillsWrapper>
        </NodeWrapper>
    );
}
