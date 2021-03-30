import React, { useEffect, useMemo, useState } from 'react';
import { useParams } from 'react-router';
import { Tree, hierarchy } from '@vx/hierarchy';
import { Group } from '@vx/group';
import { HierarchyPointNode } from '@vx/hierarchy/lib/types';
import { LinkHorizontalStep as LinkVerticalStep } from '@vx/shape';
import { LinearGradient } from '@vx/gradient';
import { useWindowSize } from '@react-hook/window-size';
import { useDrag } from '@visx/drag';
import { Zoom } from '@vx/zoom';

import { Alert, Button, Card, Drawer } from 'antd';
import styled from 'styled-components';

import { GetDatasetQuery, useGetDatasetLazyQuery, useGetDatasetQuery } from '../../graphql/dataset.generated';
import { Message } from '../shared/Message';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import CompactContext from '../shared/CompactContext';
import { BrowsableEntityPage } from '../browse/BrowsableEntityPage';

const peach = '#fd9b93';
const pink = '#fe6e9e';
const green = 'black';
const lightpurple = '#374469';
export const background = 'white';

interface TreeNode {
    name: string;
    children?: this[];
}

type HierarchyNode = HierarchyPointNode<TreeNode>;

type LineageExplorerParams = {
    type: string;
    urn: string;
};

const ControlCard = styled(Card)`
    position: absolute;
    box-shadow: 4px 4px 4px -1px grey;
    bottom: 20px;
    right: 20px;
`;

function truncate(input, length) {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
}

/** Handles rendering Root, Parent, and other Nodes. */
function Node({
    node,
    isHighlighted,
    onEntityClick,
    onExpandClick,
    isUpstream,
}: {
    node: HierarchyNode & {
        data: {
            unexploredChildren?: number;
            urn?: string;
        };
    };
    isHighlighted: boolean;
    onEntityClick: (EntitySelectParams) => void;
    onExpandClick: (LineageExpandParams) => void;
    isUpstream?: boolean;
}) {
    const width = 140;
    const height = 80;
    const centerX = -width / 2;
    const centerY = -height / 2;

    return (
        <Group top={node.y} left={node.x} style={{ cursor: 'pointer' }}>
            {node.data.unexploredChildren && isUpstream && (
                <g
                    style={{ cursor: 'pointer' }}
                    onClick={() => {
                        onExpandClick({ urn: node.data.urn, type: EntityType.Dataset, isUpstream: true });
                    }}
                >
                    <circle fill={background} cy={centerY + height / 2} cx={centerX - 10} r="20" />
                    <g fill="grey" transform={`translate(${-1 * width + 37} -21.5) scale(0.04 0.04)`}>
                        <path d="M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm192 472c0 4.4-3.6 8-8 8H544v152c0 4.4-3.6 8-8 8h-48c-4.4 0-8-3.6-8-8V544H328c-4.4 0-8-3.6-8-8v-48c0-4.4 3.6-8 8-8h152V328c0-4.4 3.6-8 8-8h48c4.4 0 8 3.6 8 8v152h152c4.4 0 8 3.6 8 8v48z" />
                    </g>
                </g>
            )}
            {node.data.unexploredChildren && !isUpstream && (
                <g
                    style={{ cursor: 'pointer' }}
                    onClick={() => {
                        onExpandClick({ urn: node.data.urn, type: EntityType.Dataset, isUpstream: false });
                    }}
                >
                    <circle fill={background} cy={centerY + height / 2} cx={centerX + width + 10} r="20" />
                    <g fill="grey" transform={`translate(${width / 2 - 10} -21.5) scale(0.04 0.04)`}>
                        <path d="M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm192 472c0 4.4-3.6 8-8 8H544v152c0 4.4-3.6 8-8 8h-48c-4.4 0-8-3.6-8-8V544H328c-4.4 0-8-3.6-8-8v-48c0-4.4 3.6-8 8-8h152V328c0-4.4 3.6-8 8-8h48c4.4 0 8 3.6 8 8v152h152c4.4 0 8 3.6 8 8v48z" />
                    </g>
                </g>
            )}
            <rect
                height={height}
                width={width}
                y={centerY}
                x={centerX}
                fill={background}
                stroke={isHighlighted ? 'blue' : green}
                strokeWidth={2}
                strokeOpacity={1}
                rx={10}
                onClick={() => {
                    onEntityClick({ urn: node.data.urn, type: EntityType.Dataset });
                }}
            />
            <text
                dy=".33em"
                fontSize={14}
                fontFamily="Arial"
                textAnchor="middle"
                fill={green}
                style={{ pointerEvents: 'none' }}
            >
                {truncate(node.data.name?.split('.').slice(-1)[0], 16)}
            </text>
        </Group>
    );
}

const defaultMargin = { top: 10, left: 280, right: 280, bottom: 10 };

function constructFetchedNode(urn: string, fetchedEntities: { [x: string]: FetchedEntity }, isUpstream: boolean) {
    const fetchedNode = fetchedEntities[urn];
    if (fetchedNode) {
        return {
            name: fetchedNode.name,
            urn: fetchedNode.urn,
            type: fetchedNode.type,
            children: fetchedNode?.children?.map((child) =>
                constructFetchedNode(child.urn, fetchedEntities, isUpstream),
            ),
            unexploredChildren: fetchedNode.unexploredChildren,
        };
    }
    return {};
}

function constructTree(
    dataset: GetDatasetQuery['dataset'],
    fetchedEntities: { [x: string]: FetchedEntity },
    isUpstream: boolean,
): TreeNode {
    const root = {
        name: dataset?.name,
        urn: dataset?.urn,
        type: dataset?.type,
    } as TreeNode;

    root.children = isUpstream
        ? dataset?.upstreamLineage?.upstreams.map((upstream) =>
              constructFetchedNode(upstream.dataset.urn, fetchedEntities, isUpstream),
          )
        : dataset?.downstreamLineage?.downstreams.map((downstream) =>
              constructFetchedNode(downstream.dataset.urn, fetchedEntities, isUpstream),
          );

    return root;
}

function transformToString(transform: {
    scaleX: number;
    scaleY: number;
    translateX: number;
    translateY: number;
    skewX: number;
    skewY: number;
}): string {
    return `matrix(${transform.scaleX}, ${transform.skewX}, ${transform.skewY}, ${transform.scaleY}, ${transform.translateX}, ${transform.translateY})`;
}

export type TreeProps = {
    margin?: { top: number; right: number; bottom: number; left: number };
    dataset: GetDatasetQuery['dataset'];
    fetchedEntities: { [x: string]: FetchedEntity };
    onEntityClick: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    highlightedEntity?: EntitySelectParams;
};

function Example({
    margin = defaultMargin,
    dataset,
    fetchedEntities,
    onEntityClick,
    onLineageExpand,
    highlightedEntity,
}: TreeProps) {
    const downstreamData = useMemo(() => hierarchy(constructTree(dataset, fetchedEntities, false)), [
        dataset,
        fetchedEntities,
    ]);
    const upstreamData = useMemo(() => hierarchy(constructTree(dataset, fetchedEntities, true)), [
        dataset,
        fetchedEntities,
    ]);
    const [windowWidth, windowHeight] = useWindowSize();
    const [upstreamHeight, setUpstreamHeight] = useState(1);
    const [downstreamHeight, setDownstreamHeight] = useState(1);
    const height = windowHeight - 125;
    const width = windowWidth;
    const yMax = height - margin.top - margin.bottom;
    const xMax = (width - margin.left - margin.right) / 2;
    const drag = useDrag();
    const initialTransform = {
        scaleX: 1,
        scaleY: 1,
        translateX: width / 2,
        translateY: 0,
        skewX: 0,
        skewY: 0,
    };
    return (
        <Zoom
            width={width}
            height={height}
            scaleXMin={1 / 2}
            scaleXMax={4}
            scaleYMin={1 / 2}
            scaleYMax={4}
            transformMatrix={initialTransform}
        >
            {(zoom) => (
                <svg
                    width={width}
                    height={height}
                    onMouseDown={zoom.dragStart}
                    onMouseUp={zoom.dragEnd}
                    onMouseMove={zoom.dragMove}
                    onTouchStart={zoom.dragStart}
                    onTouchMove={zoom.dragMove}
                    onTouchEnd={zoom.dragEnd}
                    style={{ cursor: zoom.isDragging ? 'grabbing' : 'grab' }}
                >
                    <defs>
                        <marker
                            id="triangle"
                            viewBox="0 0 10 10"
                            refX="80"
                            refY="5"
                            markerUnits="strokeWidth"
                            markerWidth="10"
                            markerHeight="10"
                            orient="auto"
                        >
                            <path d="M 0 0 L 10 5 L 0 10 z" fill="#000" />
                        </marker>
                    </defs>
                    <LinearGradient id="lg" from={peach} to={pink} />
                    <rect width={width} height={height} fill={background} />
                    <Tree<TreeNode> root={upstreamData} size={[yMax, xMax * upstreamHeight]}>
                        {(tree) => {
                            console.log(tree);
                            if (tree.height > upstreamHeight) {
                                zoom.scale({
                                    scaleX: upstreamHeight / tree.height,
                                    scaleY: upstreamHeight / tree.height,
                                });
                                setUpstreamHeight(tree.height);
                            }
                            tree.descendants().forEach((descendent) => {
                                const storeX = descendent.x;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.x = descendent.y;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.y = storeX;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.y += drag.dx;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.x = -1 * descendent.x + drag.dy;
                            });
                            const alternedTransform = {
                                ...zoom.transformMatrix,
                                translateX: zoom.transformMatrix.translateX,
                            };
                            return (
                                <Group
                                    transform={transformToString(alternedTransform)}
                                    top={margin.top}
                                    left={margin.left}
                                >
                                    {tree.links().map((link) => {
                                        const newLink = { target: link.source, source: link.target };
                                        return (
                                            <LinkVerticalStep
                                                percent={0.5}
                                                data={newLink}
                                                stroke={lightpurple}
                                                strokeWidth="1"
                                                fill="none"
                                                markerEnd="url(#triangle)"
                                            />
                                        );
                                    })}
                                    {tree.descendants().map((node) => {
                                        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                                        // @ts-ignore
                                        const isHighlighted = node.data.urn === highlightedEntity?.urn;
                                        return (
                                            <Node
                                                node={node}
                                                isHighlighted={isHighlighted}
                                                onEntityClick={onEntityClick}
                                                onExpandClick={onLineageExpand}
                                                isUpstream
                                            />
                                        );
                                    })}
                                </Group>
                            );
                        }}
                    </Tree>
                    <Tree<TreeNode> root={downstreamData} size={[yMax, xMax * downstreamHeight]}>
                        {(tree) => {
                            if (tree.height > downstreamHeight) {
                                zoom.scale({
                                    scaleX: upstreamHeight / tree.height,
                                    scaleY: upstreamHeight / tree.height,
                                });
                                setDownstreamHeight(tree.height);
                            }
                            tree.descendants().forEach((descendent) => {
                                const storeX = descendent.x;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.x = descendent.y;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.y = storeX;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.y += drag.dx;
                                // eslint-disable-next-line  no-param-reassign
                                descendent.x += drag.dy;
                            });
                            return (
                                <Group
                                    top={margin.top}
                                    left={margin.left + xMax}
                                    transform={transformToString(zoom.transformMatrix)}
                                >
                                    {tree.links().map((link) => {
                                        return (
                                            <LinkVerticalStep
                                                percent={0.5}
                                                data={link}
                                                stroke={lightpurple}
                                                strokeWidth="1"
                                                fill="none"
                                                markerEnd="url(#triangle)"
                                            />
                                        );
                                    })}
                                    {tree.descendants().map((node) => {
                                        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                                        // @ts-ignore
                                        const isHighlighted = node.data.urn === highlightedEntity?.urn;
                                        return (
                                            <Node
                                                node={node}
                                                isHighlighted={isHighlighted}
                                                onEntityClick={onEntityClick}
                                                onExpandClick={onLineageExpand}
                                            />
                                        );
                                    })}
                                </Group>
                            );
                        }}
                    </Tree>
                </svg>
            )}
        </Zoom>
    );
}

type EntitySelectParams = {
    type: EntityType;
    urn: string;
};

type LineageExpandParams = {
    type: EntityType;
    urn: string;
    isUpstream: boolean;
};

type FetchedEntity = {
    urn: string;
    name: string;
    type: EntityType;
    unexploredChildren?: number;
    children?: Array<{ urn: string }>;
};

export default function LineageExplorer() {
    const { type, urn } = useParams<LineageExplorerParams>();
    const { loading, error, data } = useGetDatasetQuery({ variables: { urn } });
    const [getUpstreamDataset, { data: upstreamDatasetData }] = useGetDatasetLazyQuery();
    const [getDownstreamDataset, { data: downstreamDatasetData }] = useGetDatasetLazyQuery();
    const [isDrawerVisible, setIsDrawVisible] = useState(false);
    const [highlightedEntity, setHighlightedEntity] = useState<EntitySelectParams | undefined>(undefined);
    const entityRegistry = useEntityRegistry();
    const [fetchedEntities, setFetchedEntities] = useState<{ [x: string]: FetchedEntity }>({});

    useEffect(() => {
        if (data?.dataset?.urn && !(data?.dataset?.urn in fetchedEntities)) {
            let newFetchedEntities = fetchedEntities;
            newFetchedEntities = {
                ...newFetchedEntities,
                [data.dataset.urn]: {
                    urn: data?.dataset.urn,
                    name: data?.dataset.name,
                    type: data?.dataset.type,
                    unexploredChildren: 0,
                    children: [],
                },
            };
            data?.dataset?.upstreamLineage?.upstreams.forEach((upstream) => {
                newFetchedEntities = {
                    ...newFetchedEntities,
                    [upstream?.dataset.urn]: {
                        urn: upstream?.dataset.urn,
                        name: upstream?.dataset.name,
                        type: upstream?.dataset.type,
                        unexploredChildren: upstream?.dataset.upstreamLineage?.upstreams.length,
                        children: [],
                    },
                };
            });
            data?.dataset?.downstreamLineage?.downstreams.forEach((upstream) => {
                newFetchedEntities = {
                    ...newFetchedEntities,
                    [upstream?.dataset.urn]: {
                        urn: upstream?.dataset.urn,
                        name: upstream?.dataset.name,
                        type: upstream?.dataset.type,
                        unexploredChildren: upstream?.dataset.downstreamLineage?.downstreams.length,
                        children: [],
                    },
                };
            });
            setFetchedEntities(newFetchedEntities);
        }
    }, [data, fetchedEntities, setFetchedEntities]);

    useEffect(() => {
        if (
            upstreamDatasetData?.dataset?.urn &&
            (fetchedEntities[upstreamDatasetData?.dataset?.urn].unexploredChildren || 0) > 0
        ) {
            let newFetchedEntities = fetchedEntities;
            newFetchedEntities = {
                ...newFetchedEntities,
                [upstreamDatasetData.dataset.urn]: {
                    urn: upstreamDatasetData?.dataset.urn,
                    name: upstreamDatasetData?.dataset.name,
                    type: upstreamDatasetData?.dataset.type,
                    unexploredChildren: 0,
                    children: upstreamDatasetData?.dataset.upstreamLineage?.upstreams.map((child) => ({
                        urn: child.dataset.urn,
                    })),
                },
            };
            upstreamDatasetData?.dataset.upstreamLineage?.upstreams.forEach((upstream) => {
                newFetchedEntities = {
                    ...newFetchedEntities,
                    [upstream?.dataset.urn]: {
                        urn: upstream?.dataset.urn,
                        name: upstream?.dataset.name,
                        type: upstream?.dataset.type,
                        unexploredChildren: upstream?.dataset.upstreamLineage?.upstreams.length,
                        children: [],
                    },
                };
            });
            setFetchedEntities(newFetchedEntities);
        }
    }, [upstreamDatasetData, fetchedEntities, setFetchedEntities]);

    useEffect(() => {
        if (
            downstreamDatasetData?.dataset?.urn &&
            (fetchedEntities[downstreamDatasetData?.dataset?.urn].unexploredChildren || 0) > 0
        ) {
            let newFetchedEntities = fetchedEntities;
            newFetchedEntities = {
                ...newFetchedEntities,
                [downstreamDatasetData.dataset.urn]: {
                    urn: downstreamDatasetData?.dataset.urn,
                    name: downstreamDatasetData?.dataset.name,
                    type: downstreamDatasetData?.dataset.type,
                    unexploredChildren: 0,
                    children: downstreamDatasetData?.dataset.downstreamLineage?.downstreams.map((child) => ({
                        urn: child.dataset.urn,
                    })),
                },
            };
            downstreamDatasetData?.dataset.downstreamLineage?.downstreams.forEach((upstream) => {
                newFetchedEntities = {
                    ...newFetchedEntities,
                    [upstream?.dataset.urn]: {
                        urn: upstream?.dataset.urn,
                        name: upstream?.dataset.name,
                        type: upstream?.dataset.type,
                        unexploredChildren: upstream?.dataset.downstreamLineage?.downstreams.length,
                        children: [],
                    },
                };
            });
            setFetchedEntities(newFetchedEntities);
        }
    }, [downstreamDatasetData, fetchedEntities, setFetchedEntities]);

    console.log({ type });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <BrowsableEntityPage urn={urn} type={data?.dataset?.type as EntityType}>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data?.dataset && (
                <div>
                    <Example
                        highlightedEntity={highlightedEntity}
                        fetchedEntities={fetchedEntities}
                        dataset={data?.dataset}
                        onEntityClick={(params: EntitySelectParams) => {
                            setIsDrawVisible(true);
                            setHighlightedEntity(params);
                        }}
                        onLineageExpand={(params: LineageExpandParams) => {
                            if (params.isUpstream) {
                                getUpstreamDataset({ variables: { urn: params.urn } });
                            } else {
                                getDownstreamDataset({ variables: { urn: params.urn } });
                            }
                        }}
                    />
                </div>
            )}
            <ControlCard size="small">
                <Button href={`/${type}/${urn}`} type="link">
                    Return to {type}
                </Button>
            </ControlCard>
            <Drawer
                title="Entity Overview"
                placement="left"
                closable
                onClose={() => {
                    setIsDrawVisible(false);
                    setHighlightedEntity(undefined);
                }}
                visible={isDrawerVisible}
                width={425}
                mask={false}
            >
                <CompactContext.Provider value>
                    {highlightedEntity && entityRegistry.renderProfile(highlightedEntity.type, highlightedEntity.urn)}
                </CompactContext.Provider>
            </Drawer>
        </BrowsableEntityPage>
    );
}
