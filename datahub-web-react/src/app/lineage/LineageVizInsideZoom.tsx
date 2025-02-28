import React, { useEffect, useState } from 'react';
import { PlusOutlined, MinusOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { Button } from 'antd';
import { ProvidedZoom, TransformMatrix } from '@visx/zoom/lib/types';

import { ColumnEdge, EntityAndType, EntitySelectParams, FetchedEntity } from './types';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { SchemaField, SchemaFieldRef } from '../../types.generated';
import { useIsShowColumnsMode } from './utils/useIsShowColumnsMode';
import { LineageVizControls } from './controls/LineageVizControls';
import LineageVizRootSvg from './LineageVizRootSvg';

const ControlsDiv = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    height: 60px;
`;

const ZoomContainer = styled.div`
    position: relative;
`;

const ZoomControls = styled.div`
    position: absolute;
    top: 20px;
    right: 20px;
`;

const ZoomButton = styled(Button)`
    display: block;
    margin-bottom: 12px;
`;

type Props = {
    margin: { top: number; right: number; bottom: number; left: number };
    entityAndType?: EntityAndType | null;
    fetchedEntities: Map<string, FetchedEntity>;
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (data: EntityAndType) => void;
    selectedEntity?: EntitySelectParams;
    zoom: ProvidedZoom<any> & {
        transformMatrix: TransformMatrix;
        isDragging: boolean;
    };
    width: number;
    height: number;
    fineGrainedMap?: any;
    refetchCenterNode: () => void;
};

export default function LineageVizInsideZoom({
    zoom,
    margin,
    entityAndType,
    fetchedEntities,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    width,
    height,
    fineGrainedMap,
    refetchCenterNode,
}: Props) {
    const [collapsedColumnsNodes, setCollapsedColumnsNodes] = useState<Record<string, boolean>>({});
    const [selectedField, setSelectedField] = useState<SchemaFieldRef | null>(null);
    const [highlightedEdges, setHighlightedEdges] = useState<ColumnEdge[]>([]);
    const [visibleColumnsByUrn, setVisibleColumnsByUrn] = useState<Record<string, Set<string>>>({});
    const [columnsByUrn, setColumnsByUrn] = useState<Record<string, SchemaField[]>>({});
    const [showExpandedTitles, setShowExpandedTitles] = useState(false);
    const showColumns = useIsShowColumnsMode();

    useEffect(() => {
        zoom.setTransformMatrix({ ...zoom.transformMatrix, translateY: 0, translateX: width / 2 });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityAndType?.entity?.urn]);

    return (
        <>
            <LineageExplorerContext.Provider
                value={{
                    expandTitles: showExpandedTitles,
                    showColumns,
                    collapsedColumnsNodes,
                    setCollapsedColumnsNodes,
                    fineGrainedMap,
                    selectedField,
                    setSelectedField,
                    highlightedEdges,
                    setHighlightedEdges,
                    visibleColumnsByUrn,
                    setVisibleColumnsByUrn,
                    columnsByUrn,
                    setColumnsByUrn,
                    refetchCenterNode,
                }}
            >
                <ControlsDiv>
                    <LineageVizControls
                        showExpandedTitles={showExpandedTitles}
                        setShowExpandedTitles={setShowExpandedTitles}
                    />
                </ControlsDiv>
                <ZoomContainer>
                    <ZoomControls>
                        <ZoomButton onClick={() => zoom.scale({ scaleX: 1.2, scaleY: 1.2 })}>
                            <PlusOutlined />
                        </ZoomButton>
                        <Button onClick={() => zoom.scale({ scaleX: 0.8, scaleY: 0.8 })}>
                            <MinusOutlined />
                        </Button>
                    </ZoomControls>
                    <LineageVizRootSvg
                        entityAndType={entityAndType}
                        width={width}
                        height={height}
                        margin={margin}
                        onEntityClick={onEntityClick}
                        onEntityCenter={onEntityCenter}
                        onLineageExpand={onLineageExpand}
                        selectedEntity={selectedEntity}
                        zoom={zoom}
                        fetchedEntities={fetchedEntities}
                    />
                </ZoomContainer>
            </LineageExplorerContext.Provider>
        </>
    );
}
