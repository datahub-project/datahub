import React, { useContext, useState } from 'react';
import { Trans } from 'react-i18next';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';

import { useAvoidIntersectionsOften } from '@app/lineageV3/LineageEntityNode/useAvoidIntersections';
import LineageFilterSearch from '@app/lineageV3/LineageFilterNode/LineageFilterSearch';
import LineageFilterSummary from '@app/lineageV3/LineageFilterNode/LineageFilterSummary';
import { ShowMoreButton } from '@app/lineageV3/LineageFilterNode/ShowMoreButton';
import useFetchFilterNodeContents from '@app/lineageV3/LineageFilterNode/useFetchFilterNodeContents';
import {
    LINEAGE_NODE_WIDTH,
    LineageFilter,
    LineageNodesContext,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';

import { EntityType } from '@types';

export const LINEAGE_FILTER_NODE_NAME = 'lineage-filter';

const NodeWrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    color: ${(props) => props.theme.colors.text};
    cursor: pointer;
    font-size: 12px;
    line-height: 16px;
    padding: 8px;
    width: ${LINEAGE_NODE_WIDTH}px;
`;

const ExtraCard = styled.div<{ bottom: number }>`
    background-color: ${(props) => props.theme.colors.bg};
    border: 1px solid ${(props) => props.theme.colors.border};
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
    font-weight: 700;
    margin: 0 1px;
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: 0px; top: 50%;' : 'right: 0; top: 50%;')}
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

    useAvoidIntersectionsOften(id, numMatches ? 153 : 137, rootType);

    const numerator = numShown ?? shown.size;
    const denominator = showGhostEntities ? allChildren.size : (total ?? allChildren.size);
    return (
        <NodeWrapper>
            <ExtraCard className="extra-card" bottom={-3} />
            <ExtraCard className="extra-card" bottom={-6} />
            <CustomHandle type="target" position={Position.Left} isConnectable={false} />
            <CustomHandle type="source" position={Position.Right} isConnectable={false} />
            <TitleWrapper>
                <Title data-testid="title">
                    <Trans
                        i18nKey="lineage:filter.shownOfTotal"
                        values={{
                            shown: Math.min(numerator, denominator),
                            total: denominator,
                            plus: showGhostEntities ? '+' : '',
                        }}
                        components={{
                            shown: <TitleCount />,
                            total: <TitleCount />,
                        }}
                    />
                </Title>
                <ShowMoreButton data={data} numMatches={numMatches} />
            </TitleWrapper>
            <LineageFilterSearch data={data} numMatches={numMatches} setNumMatches={setNumMatches} />
            <LineageFilterSummary platforms={platforms} subtypes={subtypes} />
        </NodeWrapper>
    );
}
